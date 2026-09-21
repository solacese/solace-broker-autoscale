package messaging

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrFull = errors.New("durable outbox is full; publication was not accepted")
var ErrClosed = errors.New("messaging client is closed")

type record struct {
	ID        string          `json:"event_id"`
	Shard     string          `json:"shard"`
	Partition int             `json:"partition"`
	Topic     string          `json:"topic"`
	Data      json.RawMessage `json:"data"`
	Sequence  uint64          `json:"sequence"`
}

func (r record) lane() string { return fmt.Sprintf("%s/%d", r.Shard, r.Partition) }
func number(n uint64) []byte  { b := make([]byte, 8); binary.BigEndian.PutUint64(b, n); return b }
func count(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

type outbox struct {
	db                *bolt.DB
	maxBytes, maxRows uint64
}

func openOutbox(path string, cfg config, maxBytes, maxRows uint64) (*outbox, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	o := &outbox{db, maxBytes, maxRows}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"meta", "lanes", "ids"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		m := tx.Bucket([]byte("meta"))
		previous := m.Get([]byte("contract"))
		if previous != nil {
			var old config
			if err := json.Unmarshal(previous, &old); err != nil {
				return err
			}
			if !compatible(old, cfg) {
				return errors.New("routing contract changed; preserve the original outbox and policy")
			}
		}
		data, err := json.Marshal(cfg.contract())
		if err != nil {
			return err
		}
		return m.Put([]byte("contract"), data)
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return o, nil
}
func (o *outbox) enqueue(r record) error {
	return o.db.Update(func(tx *bolt.Tx) error {
		ids := tx.Bucket([]byte("ids"))
		identity, _ := json.Marshal(r)
		if prior := ids.Get([]byte(r.ID)); prior != nil {
			if bytes.Equal(prior, identity) {
				return nil
			}
			return errors.New("pending event ID reused with different content or routing")
		}
		meta := tx.Bucket([]byte("meta"))
		size := count(meta.Get([]byte("bytes")))
		rows := count(meta.Get([]byte("rows")))
		if rows >= o.maxRows || size+uint64(len(identity)) > o.maxBytes {
			return ErrFull
		}
		lanes := tx.Bucket([]byte("lanes"))
		lane, err := lanes.CreateBucketIfNotExists([]byte(r.lane()))
		if err != nil {
			return err
		}
		r.Sequence, err = meta.NextSequence()
		if err != nil {
			return err
		}
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if err = lane.Put(number(r.Sequence), data); err != nil {
			return err
		}
		if err = ids.Put([]byte(r.ID), identity); err != nil {
			return err
		}
		if err = meta.Put([]byte("bytes"), number(size+uint64(len(identity)))); err != nil {
			return err
		}
		return meta.Put([]byte("rows"), number(rows+1))
	})
}
func (o *outbox) heads() ([]record, error) {
	var result []record
	err := o.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("lanes")).ForEach(func(k, v []byte) error {
			lane := tx.Bucket([]byte("lanes")).Bucket(k)
			_, data := lane.Cursor().First()
			if data == nil {
				return nil
			}
			var r record
			if err := json.Unmarshal(data, &r); err != nil {
				return err
			}
			result = append(result, r)
			return nil
		})
	})
	return result, err
}
func (o *outbox) accepted(r record) error {
	return o.db.Update(func(tx *bolt.Tx) error {
		lanes := tx.Bucket([]byte("lanes"))
		lane := lanes.Bucket([]byte(r.lane()))
		if lane == nil {
			return errors.New("outbox lane missing")
		}
		key, _ := lane.Cursor().First()
		if !bytes.Equal(key, number(r.Sequence)) {
			return errors.New("outbox head changed")
		}
		ids := tx.Bucket([]byte("ids"))
		size := len(ids.Get([]byte(r.ID)))
		if err := lane.Delete(key); err != nil {
			return err
		}
		if err := ids.Delete([]byte(r.ID)); err != nil {
			return err
		}
		if key, _ := lane.Cursor().First(); key == nil {
			if err := lanes.DeleteBucket([]byte(r.lane())); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("meta"))
		if err := meta.Put([]byte("bytes"), number(count(meta.Get([]byte("bytes")))-uint64(size))); err != nil {
			return err
		}
		return meta.Put([]byte("rows"), number(count(meta.Get([]byte("rows")))-1))
	})
}
func (o *outbox) pending() (uint64, error) {
	var n uint64
	err := o.db.View(func(tx *bolt.Tx) error { n = count(tx.Bucket([]byte("meta")).Get([]byte("rows"))); return nil })
	return n, err
}
