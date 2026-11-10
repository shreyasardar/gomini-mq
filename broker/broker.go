package broker

import (
	"errors"
	"fmt"
	"gominimq/models"
	"gominimq/topic"
	"os"
	"path/filepath"
	"strings"
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

// Recover discovers existing topic logs and restores their messages.
// Call this during startup, before accepting requests.
func (b *Broker) Recover() error {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()

	if b.closed {
		return fmt.Errorf("broker is closed")
	}

	if err := os.MkdirAll(b.dataDir, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		topicName := strings.TrimSuffix(entry.Name(), ".log")

		if topicName == "" {
			continue
		}

		if err := b.createTopic(topicName); err != nil {
			return fmt.Errorf("failed to recover topic %q: %w", topicName, err)
		}
	}

	return nil
}

func (b *Broker) CreateTopic(name string) error {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return fmt.Errorf("broker is closed")
	}

	return b.createTopic(name)
}

// createTopic creates a topic and recovers its persisted message IDs.
// The caller must hold lifecycleMu.
func (b *Broker) createTopic(name string) error {
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

	messages, err := t.Log.ReadAll()
	if err != nil {
		t.Close()
		return err
	}

	for _, msg := range messages {
		if msg.ID >= b.nextID {
			b.nextID = msg.ID + 1
		}
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

// Publish persists and enqueues a new message.
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

// Deliver moves the next available message into the in-flight state.
func (b *Broker) Deliver(topicName string) *models.Message {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return nil
	}

	t, ok := b.GetTopic(topicName)
	if !ok {
		return nil
	}

	return t.Queue.Deliver()
}

// Ack confirms successful processing of a delivered message.
// Ack confirms successful processing and persists the ACK.
func (b *Broker) Ack(topicName string, messageID int64) bool {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return false
	}

	t, ok := b.GetTopic(topicName)
	if !ok {
		return false
	}

	return t.Queue.AckWith(messageID, func() error {
		return t.AckLog.Append(messageID)
	})
}

// Nack returns an unsuccessfully processed message to the ready queue.
func (b *Broker) Nack(topicName string, messageID int64) bool {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()

	if b.closed {
		return false
	}

	t, ok := b.GetTopic(topicName)
	if !ok {
		return false
	}

	return t.Queue.Nack(messageID)
}

// Consume immediately removes and returns the next message.
// This is the legacy consume operation without acknowledgement.
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
