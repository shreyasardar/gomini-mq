package broker

import (
	"gominimq/models"
	"sync"
	"testing"
)

func BenchmarkConcurrentPublish(b *testing.B) {
	dataDir := b.TempDir()

	broker := NewBroker(dataDir)

	err := broker.CreateTopic("benchmark")
	if err != nil {
		b.Fatalf("failed to create topic: %v", err)
	}

	topic, ok := broker.GetTopic("benchmark")
	if !ok {
		b.Fatal("benchmark topic not found")
	}
	defer topic.Log.Close()

	msg := models.Message{
		Key:       "benchmark",
		Value:     "test-message",
		Timestamp: 123456,
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			broker.Publish("benchmark", msg)
		}
	})

	_ = sync.Mutex{}
}
