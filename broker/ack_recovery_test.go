package broker

import (
	"gominimq/models"
	"testing"
)

func TestAckedMessageNotRecoveredAfterRestart(t *testing.T) {
	dir := t.TempDir()

	b := NewBroker(dir)
	if err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	b.Publish("orders", models.Message{Key: "order-1", Value: "Laptop"})
	b.Publish("orders", models.Message{Key: "order-2", Value: "Phone"})

	msg := b.Deliver("orders")
	if msg == nil {
		t.Fatal("expected a message")
	}

	if !b.Ack("orders", msg.ID) {
		t.Fatal("ACK failed")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := NewBroker(dir)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	topic, exists := restarted.GetTopic("orders")
	if !exists {
		t.Fatal("topic was not recovered")
	}

	recovered := topic.Queue.Dequeue()

	if recovered == nil {
		t.Fatal("expected unacknowledged message")
	}

	if recovered.Key != "order-2" {
		t.Fatalf("expected order-2, got %s", recovered.Key)
	}

	if topic.Queue.Size() != 0 {
		t.Fatal("unexpected extra message recovered")
	}
}

func TestUnackedMessageRecoveredAfterRestart(t *testing.T) {
	dir := t.TempDir()

	b := NewBroker(dir)
	if err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}

	b.Publish("orders", models.Message{
		Key:   "order-1",
		Value: "Laptop",
	})

	delivered := b.Deliver("orders")
	if delivered == nil {
		t.Fatal("expected delivered message")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := NewBroker(dir)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	topic, exists := restarted.GetTopic("orders")
	if !exists {
		t.Fatal("topic was not recovered")
	}

	recovered := topic.Queue.Dequeue()

	if recovered == nil {
		t.Fatal("unacknowledged message was not recovered")
	}

	if recovered.ID != delivered.ID {
		t.Fatalf("expected ID %d, got %d", delivered.ID, recovered.ID)
	}
}

func TestNackAllowsRedelivery(t *testing.T) {
	b := NewBroker(t.TempDir())

	if err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	b.Publish("orders", models.Message{
		Key:   "order-1",
		Value: "Laptop",
	})

	first := b.Deliver("orders")
	if first == nil {
		t.Fatal("expected first delivery")
	}

	if !b.Nack("orders", first.ID) {
		t.Fatal("NACK failed")
	}

	second := b.Deliver("orders")
	if second == nil {
		t.Fatal("expected redelivery")
	}

	if second.ID != first.ID {
		t.Fatalf("expected ID %d, got %d", first.ID, second.ID)
	}
}
