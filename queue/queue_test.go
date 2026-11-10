package queue

import (
	"gominimq/models"
	"sync"
	"testing"
)

func TestConcurrentEnqueue(t *testing.T) {
	q := NewQueue()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			q.Enqueue(models.Message{
				Value: "test",
			})
		}()
	}

	wg.Wait()

	if q.Size() != 100 {
		t.Errorf("expected 100 messages, got %d", q.Size())
	}
}

func TestConcurrentEnqueueDequeue(t *testing.T) {
	q := NewQueue()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			q.Enqueue(models.Message{
				Value: "test",
			})
		}()
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			q.Dequeue()
		}()
	}

	wg.Wait()

	if !q.IsEmpty() {
		t.Errorf("expected queue to be empty, got %d messages", q.Size())
	}
}

func TestDeliverAndAck(t *testing.T) {
	q := NewQueue()

	msg := models.Message{
		ID:    1,
		Key:   "order-1",
		Value: "test order",
	}

	q.Enqueue(msg)

	delivered := q.Deliver()

	if delivered == nil {
		t.Fatal("expected message to be delivered")
	}

	if delivered.ID != 1 {
		t.Errorf("expected message ID 1, got %d", delivered.ID)
	}

	if q.Size() != 0 {
		t.Errorf("expected ready queue to be empty, got %d", q.Size())
	}

	if q.InFlightSize() != 1 {
		t.Errorf("expected 1 in-flight message, got %d", q.InFlightSize())
	}

	if !q.Ack(1) {
		t.Fatal("expected ACK to succeed")
	}

	if q.InFlightSize() != 0 {
		t.Errorf("expected no in-flight messages, got %d", q.InFlightSize())
	}

	if q.Ack(1) {
		t.Error("duplicate ACK should fail")
	}
}

func TestDeliverAndNack(t *testing.T) {
	q := NewQueue()

	msg := models.Message{
		ID:    2,
		Key:   "order-2",
		Value: "retry order",
	}

	q.Enqueue(msg)

	delivered := q.Deliver()

	if delivered == nil {
		t.Fatal("expected message to be delivered")
	}

	if !q.Nack(2) {
		t.Fatal("expected NACK to succeed")
	}

	if q.InFlightSize() != 0 {
		t.Errorf("expected no in-flight messages, got %d", q.InFlightSize())
	}

	if q.Size() != 1 {
		t.Errorf("expected message to return to ready queue, got %d", q.Size())
	}

	retried := q.Dequeue()

	if retried == nil {
		t.Fatal("expected message to be available again")
	}

	if retried.ID != 2 {
		t.Errorf("expected retried message ID 2, got %d", retried.ID)
	}

	if q.Nack(2) {
		t.Error("NACK on a non-in-flight message should fail")
	}
}

func TestDeliverEmptyQueue(t *testing.T) {
	q := NewQueue()

	if msg := q.Deliver(); msg != nil {
		t.Errorf("expected nil for empty queue, got %+v", msg)
	}

	if q.Ack(999) {
		t.Error("ACK for unknown message should fail")
	}
}
