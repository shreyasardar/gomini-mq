package broker

import (
	"errors"
	"fmt"
	"gominimq/models"
	"gominimq/topic"
	"path/filepath"
	"sync"
)

type Broker struct {
	topics      map[string]*topic.Topic
	nextID      int64
	dataDir     string
	mu          sync.RWMutex
	closed      bool
	lifecycleMu sync.RWMutex
}

func NewBroker(dataDir string) *Broker {
	return &Broker{
		topics:  make(map[string]*topic.Topic),
		nextID:  1,
		dataDir: dataDir,
	}
}

func (b *Broker) CreateTopic(name string) error {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return fmt.Errorf("broker is closed")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil
	}

	logFilename := filepath.Join(b.dataDir, name+".log")

	t, err := topic.NewTopic(name, logFilename)
	if err != nil {
		return err
	}

	b.topics[name] = t
	return nil
}

func (b *Broker) GetTopic(name string) (*topic.Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[name]
	if !exists {
		return nil, false
	}

	return t, true
}

func (b *Broker) Publish(topicName string, msg models.Message) bool {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return false
	}

	t, ok := b.GetTopic(topicName)
	if !ok {
		return false
	}

	b.mu.Lock()
	msg.ID = b.nextID
	b.nextID++
	b.mu.Unlock()

	if err := t.Log.Append(msg); err != nil {
		return false
	}

	t.Queue.Enqueue(msg)
	return true
}

func (b *Broker) Consume(topicName string) *models.Message {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return nil
	}

	t, ok := b.GetTopic(topicName)
	if !ok {
		return nil
	}

	return t.Queue.Dequeue()
}

func (b *Broker) Close() error {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true

	b.mu.Lock()
	defer b.mu.Unlock()

	var closeErr error

	for _, t := range b.topics {
		if err := t.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
}
