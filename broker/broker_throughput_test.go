package broker

import (
	"fmt"
	"gominimq/models"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishThroughput(t *testing.T) {
	producerConfigs := []int{1, 10, 50, 100}
	const messagesPerProducer = 1000

	for _, producers := range producerConfigs {
		t.Run(fmt.Sprintf("producers_%d", producers), func(t *testing.T) {
			b := NewBroker(t.TempDir())

			if err := b.CreateTopic("benchmark"); err != nil {
				t.Fatal(err)
			}
			defer b.Close()

			topic, ok := b.GetTopic("benchmark")
			if !ok {
				t.Fatal("benchmark topic not found")
			}

			msg := models.Message{
				Key:       "benchmark",
				Value:     "test-message",
				Timestamp: time.Now().Unix(),
			}

			totalMessages := producers * messagesPerProducer

			var wg sync.WaitGroup
			var failures atomic.Int64

			start := time.Now()

			for i := 0; i < producers; i++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for j := 0; j < messagesPerProducer; j++ {
						if !b.Publish("benchmark", msg) {
							failures.Add(1)
						}
					}
				}()
			}

			wg.Wait()

			elapsed := time.Since(start)
			throughput := float64(totalMessages) / elapsed.Seconds()

			t.Logf("Producers: %d", producers)
			t.Logf("Total messages: %d", totalMessages)
			t.Logf("Execution time: %v", elapsed)
			t.Logf("Throughput: %.2f messages/sec", throughput)
			t.Logf("Failed publishes: %d", failures.Load())

			if failures.Load() != 0 {
				t.Fatalf("publish failures: %d", failures.Load())
			}

			if topic.Queue.Size() != totalMessages {
				t.Errorf(
					"expected %d messages, got %d",
					totalMessages,
					topic.Queue.Size(),
				)
			}
		})
	}
}
