package messaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

type route struct {
	Pattern   string `json:"pattern"`
	Shard     string `json:"shard"`
	Dispatch  string `json:"dispatch"`
	KeyLevels []int  `json:"key_levels"`
}
type config struct {
	Fleet      string              `json:"fleet_id"`
	Partitions int                 `json:"partitions"`
	Routes     []route             `json:"routes"`
	Groups     map[string][]string `json:"groups,omitempty"`
	Ready      []string            `json:"ready_groups,omitempty"`
}

func (c config) contract() config {
	return config{Fleet: c.Fleet, Partitions: c.Partitions, Routes: c.Routes}
}
func compatible(old, next config) bool {
	if old.Fleet != next.Fleet || old.Partitions != next.Partitions {
		return false
	}
	for _, r := range old.Routes {
		a, _ := json.Marshal(r)
		found := false
		for _, n := range next.Routes {
			b, _ := json.Marshal(n)
			if bytes.Equal(a, b) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func (c config) validate() error {
	if c.Fleet == "" || c.Partitions < 1 || c.Partitions > 4096 || len(c.Routes) == 0 {
		return errors.New("invalid managed messaging contract")
	}
	for _, r := range c.Routes {
		if r.Shard == "" || r.Pattern == "" {
			return errors.New("invalid route")
		}
		switch r.Dispatch {
		case "", "by-key":
			if len(r.KeyLevels) == 0 {
				return errors.New("missing key levels")
			}
		case "by-topic", "single":
		default:
			return errors.New("unsupported dispatch policy")
		}
	}
	return nil
}
func matches(pattern, topic string) bool {
	p, t := strings.Split(pattern, "/"), strings.Split(topic, "/")
	for i, v := range p {
		if i >= len(t) {
			return false
		}
		if v == ">" {
			return true
		}
		if v != "*" && v != t[i] {
			return false
		}
	}
	return len(p) == len(t)
}

// jsonStrings matches Python's compact json.dumps string escaping. The inner
// business key uses ensure_ascii=True; the outer partition tuple uses False.
func jsonStrings(parts []string, ascii bool) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, s := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		for _, r := range s {
			switch r {
			case '"', '\\':
				b.WriteByte('\\')
				b.WriteRune(r)
			case '\b':
				b.WriteString(`\b`)
			case '\f':
				b.WriteString(`\f`)
			case '\n':
				b.WriteString(`\n`)
			case '\r':
				b.WriteString(`\r`)
			case '\t':
				b.WriteString(`\t`)
			default:
				if r < 32 || (ascii && r >= 127) {
					if r > 0xffff {
						a, z := utf16.EncodeRune(r)
						fmt.Fprintf(&b, `\u%04x\u%04x`, a, z)
					} else {
						fmt.Fprintf(&b, `\u%04x`, r)
					}
				} else {
					b.WriteRune(r)
				}
			}
		}
		b.WriteByte('"')
	}
	b.WriteByte(']')
	return b.String()
}
func partitionFor(shard, key string, n int) int {
	digest := sha256.Sum256([]byte(jsonStrings([]string{shard, key}, false)))
	return int(new(big.Int).Mod(new(big.Int).SetBytes(digest[:]), big.NewInt(int64(n))).Int64())
}
func (c config) publication(topic, id string, data any) (record, error) {
	if topic == "" || len(topic) > 128 || !utf8.ValidString(topic) || strings.HasPrefix(topic, "#") || strings.ContainsAny(topic, "*>\x00") {
		return record{}, errors.New("requires concrete UTF-8 topic of at most 128 bytes")
	}
	levels := strings.Split(topic, "/")
	for _, l := range levels {
		if l == "" {
			return record{}, errors.New("empty topic level")
		}
	}
	if id == "" || len(id) > 512 || !utf8.ValidString(id) {
		return record{}, errors.New("event ID must be 1-512 UTF-8 bytes")
	}
	var selected *route
	for _, r := range c.Routes {
		if matches(r.Pattern, topic) {
			if selected != nil {
				return record{}, errors.New("ambiguous route")
			}
			copy := r
			selected = &copy
		}
	}
	if selected == nil {
		return record{}, errors.New("no configured route")
	}
	ready := map[string]bool{}
	for _, g := range c.Ready {
		ready[g] = true
	}
	matched := false
	for group, patterns := range c.Groups {
		for _, p := range patterns {
			if matches(p, topic) {
				matched = true
				if !ready[group] {
					return record{}, errors.New("matching subscriber group is not ready")
				}
			}
		}
	}
	if !matched {
		return record{}, errors.New("no matching subscriber group")
	}
	var keys []string
	switch selected.Dispatch {
	case "by-topic":
		keys = []string{"topic", topic}
	case "single":
		keys = []string{"route", selected.Pattern}
	default:
		for _, i := range selected.KeyLevels {
			if i < 0 || i >= len(levels) {
				return record{}, errors.New("invalid key level")
			}
			keys = append(keys, levels[i])
		}
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return record{}, err
	}
	if len(payload) > 1<<20 {
		return record{}, errors.New("payload exceeds 1 MiB")
	}
	return record{ID: id, Shard: selected.Shard, Partition: partitionFor(selected.Shard, jsonStrings(keys, true), c.Partitions), Topic: topic, Data: payload}, nil
}
