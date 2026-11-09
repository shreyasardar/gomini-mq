package storage

import (
	"bufio"
	"encoding/json"
	"gominimq/models"
	"os"
	"path/filepath"
	"sync"
)

type Log struct {
	file   *os.File
	mu     sync.Mutex
	closed bool
}

func NewLog(filename string) (*Log, error) {

	dir := filepath.Dir(filename)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &Log{
		file: file,
	}, nil
}

func (l *Log) Append(msg models.Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return os.ErrClosed
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	_, err = l.file.Write(data)

	return err
}

func (l *Log) ReadAll() ([]models.Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, os.ErrClosed
	}

	file, err := os.Open(l.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []models.Message

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var msg models.Message

		err := json.Unmarshal(scanner.Bytes(), &msg)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	return l.file.Close()
}
