// Package amqp is the real AMQP 1.0 Transport for the smart shim, built on github.com/Azure/go-amqp.
// It is the production counterpart to transport/memory: the shim logic in package dispatch is written
// against the dispatch.Transport interface, so swapping this in for the in-memory transport is the
// only change needed to talk to real Solace brokers.
//
// A connection URI carries everything needed to dial one broker, for example
// amqps://user:pass@host.messaging.solace.cloud:5671. The scheme selects TLS (amqps) or plaintext
// (amqp); any userinfo becomes SASL PLAIN credentials. One Transport dials a fresh connection per
// Sender/Receiver call; the shim's own connection cache keeps that to one sender per (broker, uri).
package amqp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"sync"

	goamqp "github.com/Azure/go-amqp"
	"github.com/solacese/solace-broker-autoscale/shim/dispatch"
)

// Transport dials real AMQP 1.0 brokers. The zero value is ready to use; New is provided for
// symmetry with the in-memory transport and to set an optional TLS config.
type Transport struct {
	// TLSConfig is applied to amqps connections. Nil uses a default config (server cert verified
	// against the system roots). Set this to pin certificates or, in a lab, to skip verification.
	TLSConfig *tls.Config
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

// Sender opens a connection, a session, and a sender link to the broker at uri. The link target is
// set per message (go-amqp allows an anonymous sender with the address on the message), so one sender
// serves every address on the broker.
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

// Receiver opens a connection, session, and receiver link bound to source (a topic or queue).
func (t *Transport) Receiver(ctx context.Context, uri, source string) (dispatch.Receiver, error) {
	conn, err := t.dial(ctx, uri)
	if err != nil {
		return nil, err
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open session: %w", err)
	}
	link, err := session.NewReceiver(ctx, source, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open receiver on %q: %w", source, err)
	}
	return &receiver{conn: conn, link: link}, nil
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
	conn *goamqp.Conn
	link *goamqp.Receiver
}

func (r *receiver) Receive(ctx context.Context) (dispatch.Message, error) {
	m, err := r.link.Receive(ctx, nil)
	if err != nil {
		return dispatch.Message{}, err
	}
	// Receiving is not successful application processing. Leave settlement to the caller.
	out := dispatch.Message{Body: m.GetData()}
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

func (r *receiver) Close() error { return r.conn.Close() }
