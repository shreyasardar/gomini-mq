package queue

import (
	"fmt"
	"gominimq/models"
	"sync"
)

type Queue struct {
	Messages      []models.Message
	inFlight      map[int64]models.Message
	inFlightOrder []int64
	mu            sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{
		Messages:      []models.Message{},
		inFlight:      make(map[int64]models.Message),
		inFlightOrder: []int64{},
	}
}

func (q *Queue) Enqueue(msg models.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.Messages = append(q.Messages, msg)
}

func (q *Queue) Dequeue() *models.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	return &msg
}

func (q *Queue) Deliver() *models.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	q.Messages = q.Messages[1:]

	q.inFlight[msg.ID] = msg
	q.inFlightOrder = append(q.inFlightOrder, msg.ID)

	return &msg
}

// AckWith persists an ACK before removing the message from in-flight.
func (q *Queue) AckWith(id int64, persist func() error) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.inFlight[id]; !exists {
		return false
	}

	if persist != nil {
		if err := persist(); err != nil {
			return false
		}
	}

	delete(q.inFlight, id)
	q.removeInFlightOrder(id)

	return true
}

func (q *Queue) Ack(id int64) bool {
	return q.AckWith(id, nil)
}

func (q *Queue) Nack(id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, exists := q.inFlight[id]
	if !exists {
		return false
	}

	delete(q.inFlight, id)
	q.removeInFlightOrder(id)

	q.Messages = append([]models.Message{msg}, q.Messages...)

	return true
}

func (q *Queue) removeInFlightOrder(id int64) {
	for i, currentID := range q.inFlightOrder {
		if currentID == id {
			q.inFlightOrder = append(
				q.inFlightOrder[:i],
				q.inFlightOrder[i+1:]...,
			)
			return
		}
	}
}

func (q *Queue) Peek() *models.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.Messages) == 0 {
		return nil
	}

	msg := q.Messages[0]
	return &msg
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.Messages)
}

func (q *Queue) InFlightSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inFlight)
}

func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *Queue) Print() {
	q.mu.Lock()
	defer q.mu.Unlock()

	fmt.Println("Queue Messages:")
	fmt.Println("----------------")

	for _, msg := range q.Messages {
		fmt.Println(msg)
	}
}
