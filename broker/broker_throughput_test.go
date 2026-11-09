package broker

import (
	"gominimq/models"
	"sync"
	"testing"
	"time"
)

func TestPublishThroughput(t *testing.T) {
	dataDir := t.TempDir()

	broker := NewBroker(dataDir)

	err := broker.CreateTopic("benchmark")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	topic, ok := broker.GetTopic("benchmark")
	if !ok {
		t.Fatal("benchmark topic not found")
	}
	defer topic.Log.Close()

	const producers = 100
	const messagesPerProducer = 1000

	msg := models.Message{
		Key:       "benchmark",
		Value:     "test-message",
		Timestamp: time.Now().Unix(),
	}

	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < producers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < messagesPerProducer; j++ {
				if !broker.Publish("benchmark", msg) {
					t.Error("publish failed")
				}
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)

	totalMessages := producers * messagesPerProducer
	throughput := float64(totalMessages) / elapsed.Seconds()

	t.Logf("Total messages: %d", totalMessages)
	t.Logf("Total time: %v", elapsed)
	t.Logf("Throughput: %.2f messages/sec", throughput)

	if topic.Queue.Size() != totalMessages {
		t.Errorf(
			"expected %d messages in queue, got %d",
			totalMessages,
			topic.Queue.Size(),
		)
	}
}
