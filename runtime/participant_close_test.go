package runtime

import (
	"reflect"
	"testing"
)

func TestPublisherResourceCloseOrder(t *testing.T) {
	var order []string
	owners := &CloseGroup{Closers: []interface{ Close() error }{
		closeRecorder{name: "data-service", order: &order},
		closeRecorder{name: "outbox", order: &order},
		closeRecorder{name: "native-publisher", order: &order},
		closeRecorder{name: "dispatcher", order: &order},
	}}
	if err := owners.Close(); err != nil {
		t.Fatal(err)
	}
	want := []string{"dispatcher", "native-publisher", "outbox", "data-service"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("close order = %v, want %v", order, want)
	}
}
