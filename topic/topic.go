package topic

import (
	"errors"
	"gominimq/queue"
	"gominimq/storage"
	"path/filepath"
)

type Topic struct {
	Name   string
	Queue  *queue.Queue
	Log    *storage.Log
	AckLog *storage.AckLog
}

func NewTopic(name string, logFilename string) (*Topic, error) {
	// Open message log.
	log, err := storage.NewLog(logFilename)
	if err != nil {
		return nil, err
	}

	// Read persisted messages.
	messages, err := log.ReadAll()
	if err != nil {
		log.Close()
		return nil, err
	}

	// Open the corresponding ACK log.
	ackFilename := filepath.Join(
		filepath.Dir(logFilename),
		name+".ack",
	)

	ackLog, err := storage.NewAckLog(ackFilename)
	if err != nil {
		log.Close()
		return nil, err
	}

	// Read acknowledged message IDs.
	ackedIDs, err := ackLog.ReadAll()
	if err != nil {
		log.Close()
		ackLog.Close()
		return nil, err
	}

	// Build a lookup of acknowledged IDs.
	acked := make(map[int64]bool)

	for _, id := range ackedIDs {
		acked[id] = true
	}

	// Restore only unacknowledged messages.
	q := queue.NewQueue()

	for _, msg := range messages {
		if !acked[msg.ID] {
			q.Enqueue(msg)
		}
	}

	return &Topic{
		Name:   name,
		Queue:  q,
		Log:    log,
		AckLog: ackLog,
	}, nil
}

func (t *Topic) Close() error {
	var closeErr error

	if err := t.Log.Close(); err != nil {
		closeErr = err
	}

	if err := t.AckLog.Close(); err != nil {
		closeErr = errors.Join(closeErr, err)
	}

	return closeErr
}
