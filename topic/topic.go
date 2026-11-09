package topic

import (
	"gominimq/queue"
	"gominimq/storage"
)

type Topic struct {
	Name  string
	Queue *queue.Queue
	Log   *storage.Log
}

func NewTopic(name string, logFilename string) (*Topic, error) {
	log, err := storage.NewLog(logFilename)
	if err != nil {
		return nil, err
	}

	return &Topic{
		Name:  name,
		Queue: queue.NewQueue(),
		Log:   log,
	}, nil
}

func (t *Topic) Close() error {
	return t.Log.Close()
}
