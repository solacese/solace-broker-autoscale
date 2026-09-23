package messaging

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// BenchmarkOutboxDurability exercises the production outbox methods and their
// default synchronous bbolt commits. Use -benchtime=100x for a short,
// reproducible storage profile; results are specific to the filesystem used.
func BenchmarkOutboxDurability(b *testing.B) {
	for _, payloadBytes := range []int{256, 4096, 65536} {
		b.Run(fmt.Sprintf("enqueue-sync/%dB", payloadBytes), func(b *testing.B) {
			o := benchmarkOutbox(b, "enqueue.db")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := o.enqueue(benchmarkRecord(payloadBytes, i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("accepted-delete-sync/4096B", func(b *testing.B) {
		o := benchmarkOutbox(b, "accepted.db")
		records := make([]record, b.N)
		for i := range records {
			records[i] = benchmarkRecord(4096, i)
			records[i].Partition = 0
			records[i].Sequence = uint64(i + 1)
			if err := o.enqueue(records[i]); err != nil {
				b.Fatal(err)
			}
		}
		b.ResetTimer()
		for i := range records {
			if err := o.accepted(records[i]); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("enqueue-plus-delete-sync/4096B", func(b *testing.B) {
		o := benchmarkOutbox(b, "cycle.db")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r := benchmarkRecord(4096, i)
			r.Partition = 0
			r.Sequence = uint64(i + 1)
			if err := o.enqueue(r); err != nil {
				b.Fatal(err)
			}
			if err := o.accepted(r); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkOutboxNoSyncDiagnostic attributes time spent in storage sync. It is
// deliberately separate from the durability benchmark: NoSync is unsafe for
// the production outbox and must never be used as a performance setting.
func BenchmarkOutboxNoSyncDiagnostic(b *testing.B) {
	o := benchmarkOutbox(b, "nosync-diagnostic.db")
	o.db.NoSync = true
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := o.enqueue(benchmarkRecord(4096, i)); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkOutbox(b *testing.B, name string) *outbox {
	b.Helper()
	o, err := openOutbox(
		filepath.Join(b.TempDir(), name),
		testConfig(),
		1<<40,
		1<<32,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = o.db.Close() })
	return o
}

func benchmarkRecord(payloadBytes, index int) record {
	payload, _ := json.Marshal(map[string]string{"padding": strings.Repeat("x", payloadBytes)})
	return record{
		ID:        fmt.Sprintf("benchmark-%d", index),
		Shard:     "payments",
		Partition: index % 8,
		Topic:     "payments/account/created",
		Data:      payload,
	}
}
