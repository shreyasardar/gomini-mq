package topic

import (
	"gominimq/models"
	"path/filepath"
	"testing"
)

func TestTopicClose(t *testing.T) {
	logFilename := filepath.Join(t.TempDir(), "test.log")

	topic, err := NewTopic("orders", logFilename)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	if err := topic.Close(); err != nil {
		t.Fatalf("failed to close topic: %v", err)
	}

	err = topic.Log.Append(models.Message{
		Key:   "test",
		Value: "after-close",
	})

	if err == nil {
		t.Error("expected append after close to fail")
	}
}

func TestTopicMessageRecovery(t *testing.T) {
	logFilename := filepath.Join(t.TempDir(), "orders.log")

	// First instance: publish messages.
	topic1, err := NewTopic("orders", logFilename)
	if err != nil {
		t.Fatalf("failed to create first topic: %v", err)
	}

	messages := []models.Message{
		{ID: 1, Key: "order-1", Value: "first order"},
		{ID: 2, Key: "order-2", Value: "second order"},
	}

	for _, msg := range messages {
		if err := topic1.Log.Append(msg); err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	if err := topic1.Close(); err != nil {
		t.Fatalf("failed to close first topic: %v", err)
	}

	// Second instance: simulate restarting the topic.
	topic2, err := NewTopic("orders", logFilename)
	if err != nil {
		t.Fatalf("failed to recreate topic: %v", err)
	}
	defer topic2.Close()

	// Verify that previously persisted messages were recovered.
	for _, expected := range messages {
		actual := topic2.Queue.Dequeue()

		if actual == nil {
			t.Fatal("expected recovered message, got nil")
		}

		if actual.ID != expected.ID ||
			actual.Key != expected.Key ||
			actual.Value != expected.Value {
			t.Errorf("expected %+v, got %+v", expected, actual)
		}
	}

	if actual := topic2.Queue.Dequeue(); actual != nil {
		t.Errorf("expected empty queue, got %+v", actual)
	}
}
