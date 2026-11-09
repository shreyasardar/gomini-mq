package storage

import (
	"errors"
	"gominimq/models"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppend(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.log")

	log, err := NewLog(filename)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}
	defer log.Close()

	msg := models.Message{
		ID:        1,
		Key:       "user1",
		Value:     "hello",
		Timestamp: 123456,
	}

	err = log.Append(msg)
	if err != nil {
		t.Fatalf("failed to append message: %v", err)
	}

	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	expected := `{"ID":1,"Key":"user1","Value":"hello","Timestamp":123456}` + "\n"

	if string(content) != expected {
		t.Errorf("expected %q, got %q", expected, string(content))
	}
}

func TestReadAll(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.log")

	log, err := NewLog(filename)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}
	defer log.Close()

	messages := []models.Message{
		{
			ID:        1,
			Key:       "user1",
			Value:     "hello",
			Timestamp: 100,
		},
		{
			ID:        2,
			Key:       "user2",
			Value:     "world",
			Timestamp: 200,
		},
	}

	for _, msg := range messages {
		err := log.Append(msg)
		if err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	result, err := log.ReadAll()
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(result) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(result))
	}

	for i := range messages {
		if result[i] != messages[i] {
			t.Errorf("message %d does not match: expected %+v, got %+v",
				i, messages[i], result[i])
		}
	}
}

func TestLogOperationsAfterClose(t *testing.T) {
	log, err := NewLog(filepath.Join(t.TempDir(), "test.log"))
	if err != nil {
		t.Fatal(err)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	if err := log.Append(models.Message{Key: "test"}); !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed from Append, got %v", err)
	}

	if _, err := log.ReadAll(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed from ReadAll, got %v", err)
	}

	if err := log.Close(); err != nil {
		t.Errorf("second Close should succeed, got %v", err)
	}
}

func TestConcurrentAppendAndClose(t *testing.T) {
	log, err := NewLog(filepath.Join(t.TempDir(), "concurrent.log"))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				err := log.Append(models.Message{Key: "test"})
				if err != nil && !errors.Is(err, os.ErrClosed) {
					t.Errorf("unexpected append error: %v", err)
					return
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := log.Close(); err != nil {
			t.Errorf("close failed: %v", err)
		}
	}()

	wg.Wait()

	if err := log.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}
}
