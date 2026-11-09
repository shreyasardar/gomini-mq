package broker

import (
	"gominimq/models"
	"testing"
)

func BenchmarkPublish(b *testing.B) {
	broker := NewBroker(b.TempDir())

	if err := broker.CreateTopic("benchmark"); err != nil {
		b.Fatalf("failed to create topic: %v", err)
	}

	defer func() {
		if err := broker.Close(); err != nil {
			b.Errorf("failed to close broker: %v", err)
		}
	}()

	msg := models.Message{
		Key:       "benchmark",
		Value:     "test-message",
		Timestamp: 123456,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !broker.Publish("benchmark", msg) {
			b.Fatal("publish failed")
		}
	}
}

func TestBrokerGracefulShutdown(t *testing.T) {
	b := NewBroker(t.TempDir())

	if err := b.CreateTopic("orders"); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	msg := models.Message{
		Key:       "order-1",
		Value:     "test order",
		Timestamp: 123456,
	}

	if !b.Publish("orders", msg) {
		t.Fatal("publish failed before shutdown")
	}

	// Close the broker.
	if err := b.Close(); err != nil {
		t.Fatalf("failed to close broker: %v", err)
	}

	// Publishing after shutdown must fail.
	if b.Publish("orders", msg) {
		t.Error("publish succeeded after shutdown")
	}

	// Creating a topic after shutdown must fail.
	if err := b.CreateTopic("payments"); err == nil {
		t.Error("topic creation succeeded after shutdown")
	}

	// Consuming after shutdown must return nil.
	if msg := b.Consume("orders"); msg != nil {
		t.Error("consume succeeded after shutdown")
	}

	// Closing twice should be safe.
	if err := b.Close(); err != nil {
		t.Errorf("second close returned an error: %v", err)
	}
}
