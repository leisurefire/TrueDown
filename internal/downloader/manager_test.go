package downloader

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestAddTaskBeyondFormerQueueCapacityReturns(t *testing.T) {
	stateDir := t.TempDir()
	databasePath := filepath.Join(stateDir, "records.db")
	m, err := NewManager("unused", filepath.Join(stateDir, "downloads"), databasePath)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 400; i++ {
			_, duplicate, err := m.AddTask(fmt.Sprintf("https://example.test/file-%d.bin", i), "", "", nil, "", 0, Aria2Opts{})
			if err != nil {
				done <- err
				return
			}
			if duplicate {
				done <- fmt.Errorf("task %d unexpectedly reported as duplicate", i)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddTask blocked after the former 256-item channel capacity")
	}
	if got := len(m.ListTasks()); got != 400 {
		t.Fatalf("ListTasks returned %d records, want 400", got)
	}
	m.Stop()

	reopened, err := NewManager("unused", filepath.Join(stateDir, "downloads"), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Stop()
	if got := len(reopened.ListTasks()); got != 400 {
		t.Fatalf("reopened database returned %d records, want 400", got)
	}
}

func TestNormalizeRequestTreatsNilAndEmptyCollectionsEqually(t *testing.T) {
	a, err := jsonFingerprint(normalizeRequest(" https://example.test/a ", "", "", "downloads", nil, "", 0, Aria2Opts{}))
	if err != nil {
		t.Fatal(err)
	}
	b, err := jsonFingerprint(normalizeRequest("https://example.test/a", "", "", "downloads", map[string]string{}, "", 0, Aria2Opts{ExtraArgs: []string{}}))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("normalized fingerprints differ: %q != %q", a, b)
	}
}

func jsonFingerprint(identity requestIdentity) (string, error) {
	data, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
