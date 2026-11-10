
package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type AckLog struct {
	file   *os.File
	mu     sync.Mutex
	closed bool
}

// NewAckLog creates or opens an ACK log file.
func NewAckLog(filename string) (*AckLog, error) {
	dir := filepath.Dir(filename)

	if err := os.MkdirAll(dir, 0755); err != nil {
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

	return &AckLog{file: file}, nil
}

// Append stores an acknowledged message ID.
func (l *AckLog) Append(id int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return os.ErrClosed
	}

	data, err := json.Marshal(id)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if _, err := l.file.Write(data); err != nil {
		return err
	}

	// Sync ensures the ACK is flushed to disk before returning.
	return l.file.Sync()
}

// ReadAll returns all acknowledged message IDs.
func (l *AckLog) ReadAll() ([]int64, error) {
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

	var ids []int64

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var id int64

		if err := json.Unmarshal(scanner.Bytes(), &id); err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

// Close closes the ACK log.
func (l *AckLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true
	return l.file.Close()
}
