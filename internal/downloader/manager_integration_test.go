package downloader

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagerAria2Lifecycle(t *testing.T) {
	if os.Getenv("TRUEDOWN_INTEGRATION") == "" {
		t.Skip("set TRUEDOWN_INTEGRATION=1 to run the aria2 integration test")
	}
	aria2Path, err := filepath.Abs(filepath.Join("..", "..", "aria2", "aria2c.exe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(aria2Path); err != nil {
		t.Skipf("aria2 executable unavailable: %v", err)
	}

	sourceDir := t.TempDir()
	payload := []byte(strings.Repeat("TrueDown integration payload\n", 16384))
	if err := os.WriteFile(filepath.Join(sourceDir, "payload.bin"), payload, 0644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.FileServer(http.Dir(sourceDir)))
	defer server.Close()

	stateDir := t.TempDir()
	m, err := NewManager(aria2Path, filepath.Join(stateDir, "downloads"), filepath.Join(stateDir, "records.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		m.Stop()
		t.Fatal(err)
	}
	defer m.Stop()

	link := server.URL + "/payload.bin"
	task, duplicate, err := m.AddTask(link, "payload.bin", "", nil, "", 0, Aria2Opts{MaxSpeedBps: 32768})
	if err != nil || duplicate {
		t.Fatalf("first AddTask: task=%v duplicate=%v err=%v", task, duplicate, err)
	}
	same, duplicate, err := m.AddTask(link, "payload.bin", "", map[string]string{}, "", 0, Aria2Opts{MaxSpeedBps: 32768})
	if err != nil || !duplicate || same.ID != task.ID {
		t.Fatalf("duplicate AddTask: task=%v duplicate=%v err=%v", same, duplicate, err)
	}

	waitForStatus(t, m, task.ID, 8*time.Second, StatusDownloading)
	if err := m.PauseTask(task.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, m, task.ID, 4*time.Second, StatusPaused)
	if err := m.ResumeTask(task.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, m, task.ID, 20*time.Second, StatusDone)

	same, duplicate, err = m.AddTask(link, "payload.bin", "", nil, "", 0, Aria2Opts{MaxSpeedBps: 32768})
	if err != nil || !duplicate || same.ID != task.ID {
		t.Fatalf("completed duplicate AddTask: task=%v duplicate=%v err=%v", same, duplicate, err)
	}
	waitForStatus(t, m, task.ID, 8*time.Second, StatusDone)
	if count := m.ClearDone(); count != 1 {
		t.Fatalf("ClearDone()=%d, want 1", count)
	}
	if tasks := m.ListTasks(); len(tasks) != 0 {
		t.Fatalf("tasks remained after ClearDone: %v", tasks)
	}
}

func waitForStatus(t *testing.T, m *Manager, id int64, timeout time.Duration, want Status) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *Task
	for time.Now().Before(deadline) {
		last, _ = m.GetTask(id)
		if last != nil && last.Status == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("task %d did not reach %s; last=%+v", id, want, last)
}
