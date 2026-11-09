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
		t.Error("expected an error when appending to a closed log")
	}
}
