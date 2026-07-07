package downloader

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusDone        Status = "done"
	StatusError       Status = "error"
)

// Aria2Opts holds user-tunable aria2 download parameters.
type Aria2Opts struct {
	// Number of parallel connections per file (--split / --max-connection-per-server)
	Connections int `json:"connections"`
	// Max download speed in bytes/s, 0 = unlimited (--max-download-limit)
	MaxSpeedBps int `json:"maxSpeedBps"`
	// Number of retry attempts (--max-tries)
	MaxTries int `json:"maxTries"`
	// Retry wait in seconds (--retry-wait)
	RetryWait int `json:"retryWait"`
	// Extra raw aria2 flags, appended verbatim (e.g. ["--check-integrity=true"])
	ExtraArgs []string `json:"extraArgs"`
}

type Task struct {
	ID           int64             `json:"id"`
	Name         string            `json:"name"`
	Link         string            `json:"link"`
	Folder       string            `json:"folder"`
	QueueID      int               `json:"queueId"`
	Headers      map[string]string `json:"headers"`
	DownloadPage string            `json:"downloadPage,omitempty"`
	Opts         Aria2Opts         `json:"opts"`
	OutputName   string            `json:"outputName,omitempty"`
	Status       Status            `json:"status"`
	Progress     string            `json:"progress"`
	Error        string            `json:"error,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`
}

type Manager struct {
	aria2Path   string
	defaultDir  string
	tasks       map[int64]*Task
	mu          sync.RWMutex
	queue       chan *Task
	counter     atomic.Int64
	done        chan struct{}
	workerCount int
}

func NewManager(aria2Path, defaultDir string) *Manager {
	return &Manager{
		aria2Path:   aria2Path,
		defaultDir:  defaultDir,
		tasks:       make(map[int64]*Task),
		queue:       make(chan *Task, 256),
		done:        make(chan struct{}),
		workerCount: 3,
	}
}

func (m *Manager) Start() {
	if err := os.MkdirAll(m.defaultDir, 0755); err != nil {
		log.Printf("warn: could not create default dir: %v", err)
	}
	for i := 0; i < m.workerCount; i++ {
		go m.worker()
	}
}

func (m *Manager) Stop() {
	close(m.done)
}

func (m *Manager) AddTask(link, name, folder string, headers map[string]string, downloadPage string, queueID int, opts Aria2Opts) *Task {
	id := m.counter.Add(1)
	dir := folder
	if dir == "" {
		dir = m.defaultDir
	}
	if name == "" {
		name = fmt.Sprintf("task-%d", id)
	}
	t := &Task{
		ID:           id,
		Name:         name,
		Link:         link,
		Folder:       dir,
		Headers:      headers,
		DownloadPage: downloadPage,
		QueueID:      queueID,
		Opts:         opts,
		Status:       StatusQueued,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	m.mu.Lock()
	m.tasks[id] = t
	m.mu.Unlock()
	m.queue <- t
	return t
}

func (m *Manager) GetTask(id int64) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *Manager) ListTasks() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}

// RequeueTask re-enqueues a failed task. It cleans stale aria2 artifacts before retrying.
func (m *Manager) RequeueTask(id int64) error {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("task %d not found", id)
	}
	if t.Status != StatusError {
		status := t.Status
		m.mu.Unlock()
		return fmt.Errorf("task %d is %s, not error", id, status)
	}
	if err := cleanupTaskArtifacts(t.Folder, t.OutputName); err != nil {
		t.Error = fmt.Sprintf("cleanup: %v", err)
		t.UpdatedAt = time.Now()
		m.mu.Unlock()
		return err
	}
	t.Status = StatusQueued
	t.Error = ""
	t.Progress = ""
	t.UpdatedAt = time.Now()
	m.mu.Unlock()
	m.queue <- t
	return nil
}

// DeleteTask removes a done or error task from the list. Returns false if not found or still active.
func (m *Manager) DeleteTask(id int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return false
	}
	if t.Status == StatusQueued || t.Status == StatusDownloading {
		return false
	}
	delete(m.tasks, id)
	return true
}

// ClearDone removes all tasks whose status is done.
func (m *Manager) ClearDone() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, t := range m.tasks {
		if t.Status == StatusDone {
			delete(m.tasks, id)
			n++
		}
	}
	return n
}

func (m *Manager) worker() {
	for {
		select {
		case <-m.done:
			return
		case t := <-m.queue:
			m.run(t)
		}
	}
}

func (m *Manager) run(t *Task) {
	m.setStatus(t, StatusDownloading, "")

	dir := t.Folder
	if err := os.MkdirAll(dir, 0755); err != nil {
		m.setStatus(t, StatusError, fmt.Sprintf("mkdir: %v", err))
		return
	}

	o := t.Opts
	conns := o.Connections
	if conns <= 0 {
		conns = 16
	}

	args := []string{
		"--dir=" + filepath.ToSlash(dir),
		"--console-log-level=notice",
		"--summary-interval=2",
		"--download-result=full",
		fmt.Sprintf("--split=%d", conns),
		fmt.Sprintf("--max-connection-per-server=%d", conns),
	}

	if o.MaxSpeedBps > 0 {
		args = append(args, fmt.Sprintf("--max-download-limit=%d", o.MaxSpeedBps))
	}

	maxTries := o.MaxTries
	if maxTries <= 0 {
		maxTries = 5
	}
	args = append(args, fmt.Sprintf("--max-tries=%d", maxTries))

	retryWait := o.RetryWait
	if retryWait <= 0 {
		retryWait = 3
	}
	args = append(args, fmt.Sprintf("--retry-wait=%d", retryWait))

	if outputName := m.ensureOutputName(t, dir); outputName != "" {
		args = append(args, "--out="+outputName)
	}

	for k, v := range t.Headers {
		args = append(args, fmt.Sprintf("--header=%s: %s", k, v))
	}

	if t.DownloadPage != "" {
		args = append(args, "--referer="+t.DownloadPage)
	}

	args = append(args, o.ExtraArgs...)
	args = append(args, "--", t.Link)

	cmd := exec.Command(m.aria2Path, args...)
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		m.setStatus(t, StatusError, fmt.Sprintf("start: %v", err))
		return
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		m.mu.Lock()
		t.Progress = line
		t.UpdatedAt = time.Now()
		m.mu.Unlock()
		log.Printf("[task %d] %s", t.ID, line)
	}

	if err := cmd.Wait(); err != nil {
		m.setStatus(t, StatusError, err.Error())
		return
	}
	m.setStatus(t, StatusDone, "")
}

func (m *Manager) setStatus(t *Task, s Status, errMsg string) {
	m.mu.Lock()
	t.Status = s
	t.Error = errMsg
	t.UpdatedAt = time.Now()
	m.mu.Unlock()
}

func (m *Manager) ensureOutputName(t *Task, dir string) string {
	if t.Name == "" {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.OutputName == "" {
		t.OutputName = resolveOutputName(dir, t.Name)
		t.UpdatedAt = time.Now()
	}
	return t.OutputName
}

func cleanupTaskArtifacts(dir, outputName string) error {
	if outputName == "" {
		return nil
	}
	outputPath, err := safeArtifactPath(dir, outputName)
	if err != nil {
		return err
	}
	for _, path := range []string{outputPath, outputPath + ".aria2"} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

func safeArtifactPath(dir, name string) (string, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(filepath.Join(absDir, name))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe output path %q", name)
	}
	return absPath, nil
}

// resolveOutputName returns a filename that does not collide with any existing
// file in dir. If "name" is free it is returned as-is; otherwise it appends
// "(1)", "(2)", … before the extension until a free slot is found.
func resolveOutputName(dir, name string) string {
	if outputNameAvailable(dir, name) {
		return name
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 1; i < 10000; i++ {
		candidate := fmt.Sprintf("%s(%d)%s", base, i, ext)
		if outputNameAvailable(dir, candidate) {
			return candidate
		}
	}
	return name
}

func outputNameAvailable(dir, name string) bool {
	for _, path := range []string{
		filepath.Join(dir, name),
		filepath.Join(dir, name+".aria2"),
	} {
		if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
			return false
		}
	}
	return true
}
