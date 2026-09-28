package customer

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func TestMessageViewHeader(t *testing.T) {
	message := MessageView{Headers: map[string]string{"tenant": "acme"}}
	if value, ok := message.Header("tenant"); !ok || value != "acme" {
		t.Fatalf("Header() = %q, %v", value, ok)
	}
	if _, ok := message.Header("missing"); ok {
		t.Fatal("missing Header() reported present")
	}
}

func TestCustomerLibraryFuncs(t *testing.T) {
	wantHash := sha256.Sum256([]byte("bag-42"))
	library := CustomerLibraryFuncs{
		Algorithm: RendezvousSHA256Contract,
		ScalingGroup: func(message MessageView) (string, error) {
			return message.Topic, nil
		},
		BusinessHash: func(MessageView) (BusinessHash, error) {
			return wantHash, nil
		},
	}
	message := MessageView{Topic: "events-b"}
	group, err := library.GetScalingGroup(message)
	if err != nil || group != "events-b" {
		t.Fatalf("GetScalingGroup() = %q, %v", group, err)
	}
	hash, err := library.GetBusinessHash(message)
	if err != nil || hash != BusinessHash(wantHash) {
		t.Fatalf("GetBusinessHash() = %x, %v", hash, err)
	}
	if library.RoutingAlgorithm() != RendezvousSHA256Contract {
		t.Fatalf("RoutingAlgorithm() = %q", library.RoutingAlgorithm())
	}
	score, err := library.GetRendezvousScore(ScoreInput{Algorithm: RendezvousSHA256Contract, ScalingGroup: "events-b", BusinessHash: hash, BrokerID: "broker-a"})
	if err != nil || score == (RoutingScore{}) {
		t.Fatalf("GetRendezvousScore() = %x, %v", score, err)
	}
}

func TestCustomerLibraryFuncsRejectsMissingFunctions(t *testing.T) {
	library := CustomerLibraryFuncs{
		Algorithm: RendezvousSHA256Contract}
	if _, err := library.GetScalingGroup(MessageView{}); err == nil {
		t.Fatal("GetScalingGroup() accepted nil function")
	}
	if _, err := library.GetBusinessHash(MessageView{}); err == nil {
		t.Fatal("GetBusinessHash() accepted nil function")
	}
	if _, err := library.GetRendezvousScore(ScoreInput{}); err == nil {
		t.Fatal("default GetRendezvousScore() accepted invalid input")
	}
}

func TestCustomerLibraryFuncsPropagatesErrors(t *testing.T) {
	want := errors.New("customer policy rejected message")
	library := CustomerLibraryFuncs{
		Algorithm:    RendezvousSHA256Contract,
		ScalingGroup: func(MessageView) (string, error) { return "", want },
		BusinessHash: func(MessageView) (BusinessHash, error) { return BusinessHash{}, want },
	}
	if _, err := library.GetScalingGroup(MessageView{}); !errors.Is(err, want) {
		t.Fatalf("GetScalingGroup() error = %v", err)
	}
	if _, err := library.GetBusinessHash(MessageView{}); !errors.Is(err, want) {
		t.Fatalf("GetBusinessHash() error = %v", err)
	}
}
