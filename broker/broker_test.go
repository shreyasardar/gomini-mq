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
		t.Fatal(err)
	}

	msg := models.Message{
		Key:   "order-1",
		Value: "test order",
	}

	if !b.Publish("orders", msg) {
		t.Fatal("publish failed before shutdown")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	if b.Publish("orders", msg) {
		t.Error("publish succeeded after shutdown")
	}

	if err := b.CreateTopic("payments"); err == nil {
		t.Error("topic creation succeeded after shutdown")
	}

	if msg := b.Consume("orders"); msg != nil {
		t.Error("consume succeeded after shutdown")
	}

	if err := b.Close(); err != nil {
		t.Errorf("second close returned an error: %v", err)
	}
}

func TestBrokerMessageRecoveryAfterRestart(t *testing.T) {
	dataDir := t.TempDir()

	b1 := NewBroker(dataDir)

	if err := b1.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		msg := models.Message{
			Key:   "order",
			Value: "test message",
		}

		if !b1.Publish("orders", msg) {
			t.Fatalf("failed to publish message %d", i)
		}
	}

	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	b2 := NewBroker(dataDir)

	if err := b2.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	defer b2.Close()

	if !b2.Publish("orders", models.Message{
		Key:   "order-3",
		Value: "message after restart",
	}) {
		t.Fatal("failed to publish after restart")
	}

	for i := int64(1); i <= 3; i++ {
		msg := b2.Consume("orders")

		if msg == nil {
			t.Fatalf("expected message with ID %d", i)
		}

		if msg.ID != i {
			t.Errorf("expected ID %d, got %d", i, msg.ID)
		}
	}

	if msg := b2.Consume("orders"); msg != nil {
		t.Errorf("expected empty queue, got %+v", msg)
	}
}

func TestBrokerMultipleTopicRecovery(t *testing.T) {
	dataDir := t.TempDir()

	b1 := NewBroker(dataDir)

	if err := b1.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	if err := b1.CreateTopic("payments"); err != nil {
		t.Fatal(err)
	}

	if !b1.Publish("orders", models.Message{
		Key: "order-1", Value: "first",
	}) {
		t.Fatal("failed to publish order message")
	}

	if !b1.Publish("payments", models.Message{
		Key: "payment-1", Value: "second",
	}) {
		t.Fatal("failed to publish payment message")
	}

	if !b1.Publish("orders", models.Message{
		Key: "order-2", Value: "third",
	}) {
		t.Fatal("failed to publish second order message")
	}

	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Recover both topics automatically.
	b2 := NewBroker(dataDir)

	if err := b2.Recover(); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	defer b2.Close()

	if _, ok := b2.GetTopic("orders"); !ok {
		t.Fatal("orders topic was not recovered")
	}

	if _, ok := b2.GetTopic("payments"); !ok {
		t.Fatal("payments topic was not recovered")
	}

	// Publish a new message after recovery.
	if !b2.Publish("payments", models.Message{
		Key: "payment-2", Value: "fourth",
	}) {
		t.Fatal("failed to publish after recovery")
	}

	// First payment message should have ID 2.
	msg := b2.Consume("payments")

	if msg == nil || msg.ID != 2 {
		t.Fatalf("expected payment ID 2, got %+v", msg)
	}

	// New payment message should have ID 4.
	msg = b2.Consume("payments")

	if msg == nil || msg.ID != 4 {
		t.Fatalf("expected payment ID 4, got %+v", msg)
	}
}
