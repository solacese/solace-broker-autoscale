// Package amqp implements the managed client's AMQP 1.0 transport using github.com/Azure/go-amqp.
//
// A connection URI carries everything needed to dial one broker, for example
// amqps://user:pass@host.messaging.solace.cloud:5671. The scheme selects TLS (amqps) or plaintext
// (amqp); any userinfo becomes SASL PLAIN credentials. Sender connections are cached by the shim.
// Receiver links share one session and connection per URI, so partitions do not multiply connections.
package amqp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	goamqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
)

// Transport dials real AMQP 1.0 brokers. The zero value is ready to use; New is provided for
// symmetry with the in-memory transport and to set an optional TLS config.
type Transport struct {
	// TLSConfig is applied to amqps connections. Nil uses a default config (server cert verified
	// against the system roots). Set this to pin certificates or, in a lab, to skip verification.
	TLSConfig *tls.Config
	mu        sync.Mutex
	receivers map[string]*receiverSession
}

type receiverSession struct {
	conn    *goamqp.Conn
	session *goamqp.Session
	refs    int
}

// New returns a ready AMQP transport.
func New() *Transport { return &Transport{} }

func (t *Transport) dial(ctx context.Context, uri string) (*goamqp.Conn, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("parse broker uri: %w", err)
	}
	opts := &goamqp.ConnOptions{}
	if strings.EqualFold(u.Scheme, "amqps") {
		opts.TLSConfig = t.TLSConfig // nil is fine: go-amqp uses a verifying default
	}
	conn, err := goamqp.Dial(ctx, uri, opts)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", u.Redacted(), err)
	}
	return conn, nil
}

// Sender opens a connection, session, and anonymous sender link for all managed addresses.
func (t *Transport) Sender(ctx context.Context, uri string) (dispatch.Sender, error) {
	conn, err := t.dial(ctx, uri)
	if err != nil {
		return nil, err
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open session: %w", err)
	}
	// An anonymous sender (empty target) lets us set each message's To address, so a single sender
	// covers every publish address the rules produce for this broker.
	link, err := session.NewSender(ctx, "", nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open sender: %w", err)
	}
	return &sender{conn: conn, link: link}, nil
}

// Receiver opens a link bound to source, sharing the endpoint connection/session with other receivers.
func (t *Transport) Receiver(ctx context.Context, uri, source string) (dispatch.Receiver, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.receivers == nil {
		t.receivers = map[string]*receiverSession{}
	}
	pooled := t.receivers[uri]
	if pooled == nil {
		conn, err := t.dial(ctx, uri)
		if err != nil {
			return nil, err
		}
		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		pooled = &receiverSession{conn: conn, session: session}
		t.receivers[uri] = pooled
	}
	link, err := pooled.session.NewReceiver(ctx, source, &goamqp.ReceiverOptions{Credit: 1})
	if err != nil {
		var ce *goamqp.ConnError
		var se *goamqp.SessionError
		if pooled.refs == 0 || errors.As(err, &ce) || errors.As(err, &se) {
			delete(t.receivers, uri)
			_ = pooled.conn.Close()
		}
		return nil, fmt.Errorf("open receiver on %q: %w", source, err)
	}
	pooled.refs++
	return &receiver{conn: pooled.conn, link: link, transport: t, pool: pooled, uri: uri}, nil
}

type sender struct {
	conn *goamqp.Conn
	link *goamqp.Sender
}

func (s *sender) Send(ctx context.Context, msg dispatch.Message) error {
	m := goamqp.NewMessage(msg.Body)
	m.Header = &goamqp.MessageHeader{Durable: true}
	m.Properties = &goamqp.MessageProperties{}
	if msg.Address != "" {
		to := msg.Address
		m.Properties.To = &to
	}
	if msg.GroupID != "" {
		gid := msg.GroupID
		m.Properties.GroupID = &gid
	}
	if len(msg.Properties) > 0 {
		ap := make(map[string]any, len(msg.Properties))
		for k, v := range msg.Properties {
			ap[k] = v
		}
		m.ApplicationProperties = ap
	}
	return s.link.Send(ctx, m, nil)
}

func (s *sender) Close() error { return s.conn.Close() }

type receiver struct {
	conn      *goamqp.Conn
	link      *goamqp.Receiver
	transport *Transport
	pool      *receiverSession
	uri       string
	closeOnce sync.Once
}

func (r *receiver) Receive(ctx context.Context) (dispatch.Message, error) {
	m, err := r.link.Receive(ctx, nil)
	if err != nil {
		var ce *goamqp.ConnError
		var se *goamqp.SessionError
		if errors.As(err, &ce) || errors.As(err, &se) {
			r.transport.mu.Lock()
			if r.transport.receivers[r.uri] == r.pool {
				delete(r.transport.receivers, r.uri)
			}
			r.transport.mu.Unlock()
		}
		return dispatch.Message{}, err
	}
	// Receiving is not successful application processing. Leave settlement to the caller.
	body := bytes.Join(m.Data, nil)
	if len(m.Data) == 0 {
		switch value := m.Value.(type) {
		case string:
			body = []byte(value)
		case []byte:
			body = value
		}
	}
	out := dispatch.Message{Body: body}
	var mu sync.Mutex
	settled := false
	settle := func(ctx context.Context, accept bool) error {
		mu.Lock()
		defer mu.Unlock()
		if settled {
			return nil
		}
		var err error
		if accept {
			err = r.link.AcceptMessage(ctx, m)
		} else {
			err = r.link.ReleaseMessage(ctx, m)
		}
		if err == nil {
			settled = true
		}
		return err
	}
	out.Ack = func(ctx context.Context) error { return settle(ctx, true) }
	out.Release = func(ctx context.Context) error { return settle(ctx, false) }

	if m.Properties != nil {
		if m.Properties.To != nil {
			out.Address = *m.Properties.To
		}
		if m.Properties.GroupID != nil {
			out.GroupID = *m.Properties.GroupID
		}
	}
	if len(m.ApplicationProperties) > 0 {
		props := make(map[string]string, len(m.ApplicationProperties))
		for k, v := range m.ApplicationProperties {
			props[k] = fmt.Sprint(v)
		}
		out.Properties = props
	}
	return out, nil
}

func (r *receiver) Close() error {
	var result error
	r.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result = r.link.Close(ctx)
		cancel()
		t := r.transport
		t.mu.Lock()
		defer t.mu.Unlock()
		r.pool.refs--
		if r.pool.refs == 0 {
			if t.receivers[r.uri] == r.pool {
				delete(t.receivers, r.uri)
			}
			err := r.conn.Close()
			if result == nil {
				result = err
			}
		}
	})
	return result
}
