
package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAckLogAppendAndReadAll(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "orders.ack")

	log, err := NewAckLog(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	for _, id := range []int64{1, 3, 5} {
		if err := log.Append(id); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := log.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	expected := []int64{1, 3, 5}

	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("expected %v, got %v", expected, ids)
	}
}

func TestAckLogOperationsAfterClose(t *testing.T) {
	log, err := NewAckLog(filepath.Join(t.TempDir(), "orders.ack"))
	if err != nil {
		t.Fatal(err)
	}

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	if err := log.Append(1); !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	if _, err := log.ReadAll(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	if err := log.Close(); err != nil {
		t.Errorf("second Close should succeed: %v", err)
	}
}
