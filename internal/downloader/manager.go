package downloader

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusPaused      Status = "paused"
	StatusDone        Status = "done"
	StatusError       Status = "error"
)

// Aria2Opts holds user-tunable aria2 download parameters.
type Aria2Opts struct {
	Connections int      `json:"connections"`
	MaxSpeedBps int      `json:"maxSpeedBps"`
	MaxTries    int      `json:"maxTries"`
	RetryWait   int      `json:"retryWait"`
	ExtraArgs   []string `json:"extraArgs"`
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
	GID          string            `json:"gid"`
	Status       Status            `json:"status"`
	Progress     string            `json:"progress"`
	Error        string            `json:"error,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`

	Fingerprint string `json:"-"`
	RequestJSON string `json:"-"`
}

type requestIdentity struct {
	Link         string            `json:"link"`
	Name         string            `json:"name"`
	Folder       string            `json:"folder"`
	QueueID      int               `json:"queueId"`
	Headers      map[string]string `json:"headers"`
	DownloadPage string            `json:"downloadPage"`
	Opts         Aria2Opts         `json:"opts"`
}

type submission struct {
	id           int64
	recheck      bool
	removeResult bool
}

type admission struct {
	task *Task
}

type Manager struct {
	aria2Path  string
	defaultDir string
	store      *recordStore

	mu           sync.RWMutex
	tasks        map[int64]*Task
	fingerprints map[string]int64
	opMu         sync.Mutex
	nextID       atomic.Int64

	admissionMu sync.Mutex
	admissions  []admission
	dbWake      chan struct{}

	pendingMu sync.Mutex
	pending   []submission
	wake      chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
	stopOnce  sync.Once

	rpc     *ariaClient
	cmd     *exec.Cmd
	cmdDone chan error
	logFile *os.File
}

func NewManager(aria2Path, defaultDir, databasePath string) (*Manager, error) {
	store, err := openRecordStore(databasePath)
	if err != nil {
		return nil, err
	}
	tasks, err := store.LoadAll()
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("load download records: %w", err)
	}
	m := &Manager{
		aria2Path:    aria2Path,
		defaultDir:   defaultDir,
		store:        store,
		tasks:        make(map[int64]*Task, len(tasks)),
		fingerprints: make(map[string]int64, len(tasks)),
		wake:         make(chan struct{}, 1),
		dbWake:       make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
	var maxID int64
	for _, task := range tasks {
		m.tasks[task.ID] = task
		m.fingerprints[task.Fingerprint] = task.ID
		if task.ID > maxID {
			maxID = task.ID
		}
	}
	m.nextID.Store(maxID)
	m.wg.Add(1)
	go m.persistenceWriter()
	return m, nil
}

func (m *Manager) Start() error {
	if err := os.MkdirAll(m.defaultDir, 0755); err != nil {
		return fmt.Errorf("create default download directory: %w", err)
	}
	if err := m.startAria2(); err != nil {
		return err
	}
	m.wg.Add(2)
	go m.dispatcher()
	go m.poller()

	m.mu.Lock()
	for _, task := range m.tasks {
		switch task.Status {
		case StatusQueued, StatusDownloading:
			task.Status = StatusQueued
			task.Progress = "Waiting for aria2 to restore download progress"
			task.UpdatedAt = time.Now()
			if err := m.store.Update(task); err != nil {
				log.Printf("persist restored task %d: %v", task.ID, err)
			}
			m.enqueue(submission{id: task.ID, removeResult: true})
		case StatusPaused:
			m.enqueue(submission{id: task.ID, removeResult: true})
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.done)
		m.wg.Wait()
		if m.rpc != nil {
			_ = m.rpc.shutdown()
		}
		if m.cmdDone != nil {
			select {
			case <-m.cmdDone:
			case <-time.After(4 * time.Second):
				if m.cmd != nil && m.cmd.Process != nil {
					_ = m.cmd.Process.Kill()
				}
			}
		}
		if m.logFile != nil {
			_ = m.logFile.Close()
		}
		if m.store != nil {
			_ = m.store.Close()
		}
	})
}

// AddTask records the request and returns immediately. Aria2 admission runs on
// the unbounded dispatcher, so an HTTP handler never blocks on a full queue.
func (m *Manager) AddTask(link, name, folder string, headers map[string]string, downloadPage string, queueID int, opts Aria2Opts) (*Task, bool, error) {
	m.opMu.Lock()
	defer m.opMu.Unlock()

	identity := normalizeRequest(link, name, folder, m.defaultDir, headers, downloadPage, queueID, opts)
	requestJSON, err := json.Marshal(identity)
	if err != nil {
		return nil, false, err
	}
	fingerprintBytes := sha256.Sum256(requestJSON)
	fingerprint := hex.EncodeToString(fingerprintBytes[:])

	m.mu.Lock()
	if id, ok := m.fingerprints[fingerprint]; ok {
		task := m.tasks[id]
		shouldRecheck := task.Status == StatusDone || task.Status == StatusError
		if shouldRecheck {
			task.Status = StatusQueued
			task.Error = ""
			task.Progress = "Checking whether the remote content has changed"
			task.UpdatedAt = time.Now()
			if err := m.store.Update(task); err != nil {
				m.mu.Unlock()
				return nil, true, err
			}
		}
		result := cloneTask(task)
		m.mu.Unlock()
		if shouldRecheck {
			m.enqueue(submission{id: id, recheck: true, removeResult: true})
		}
		return result, true, nil
	}

	now := time.Now()
	task := &Task{
		ID:           m.nextID.Add(1),
		Fingerprint:  fingerprint,
		RequestJSON:  string(requestJSON),
		Name:         identity.Name,
		Link:         identity.Link,
		Folder:       identity.Folder,
		Headers:      identity.Headers,
		DownloadPage: identity.DownloadPage,
		QueueID:      identity.QueueID,
		Opts:         identity.Opts,
		GID:          newGID(),
		Status:       StatusQueued,
		Progress:     "Waiting for aria2",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if task.Name != "" {
		task.OutputName = resolveOutputName(task.Folder, task.Name)
	} else {
		task.Name = displayName(task.Link)
	}
	m.tasks[task.ID] = task
	m.fingerprints[fingerprint] = task.ID
	result := cloneTask(task)
	m.mu.Unlock()

	m.enqueueAdmission(admission{task: cloneTask(task)})
	return result, false, nil
}

func (m *Manager) GetTask(id int64) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[id]
	if !ok {
		return nil, false
	}
	return cloneTask(task), true
}

func (m *Manager) ListTasks() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		result = append(result, cloneTask(task))
	}
	return result
}

// RequeueTask keeps aria2's partial file and .aria2 control file so the retry
// resumes verified pieces instead of deleting progress.
func (m *Manager) RequeueTask(id int64) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	task, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("task %d not found", id)
	}
	if task.Status != StatusError {
		status := task.Status
		m.mu.Unlock()
		return fmt.Errorf("task %d is %s, not error", id, status)
	}
	task.Status = StatusQueued
	task.Error = ""
	task.Progress = "Waiting for aria2 to resume partial data"
	task.UpdatedAt = time.Now()
	err := m.store.Update(task)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	m.enqueue(submission{id: id, removeResult: true})
	return nil
}

func (m *Manager) PauseTask(id int64) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.RLock()
	task, ok := m.tasks[id]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("task %d not found", id)
	}
	status, gid := task.Status, task.GID
	m.mu.RUnlock()
	if status != StatusQueued && status != StatusDownloading {
		return fmt.Errorf("task %d is %s and cannot be paused", id, status)
	}
	if err := m.rpc.pause(gid); err != nil && !isGIDNotFound(err) {
		return err
	}
	return m.setTask(id, func(t *Task) {
		t.Status = StatusPaused
		t.Progress = "Paused by aria2"
		t.Error = ""
	})
}

func (m *Manager) ResumeTask(id int64) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.RLock()
	task, ok := m.tasks[id]
	if !ok {
		m.mu.RUnlock()
		return fmt.Errorf("task %d not found", id)
	}
	status, gid := task.Status, task.GID
	m.mu.RUnlock()
	if status != StatusPaused {
		return fmt.Errorf("task %d is %s, not paused", id, status)
	}
	err := m.rpc.unpause(gid)
	if err != nil && !isGIDNotFound(err) {
		return err
	}
	if err := m.setTask(id, func(t *Task) {
		t.Status = StatusQueued
		t.Progress = "Waiting for aria2 to resume"
		t.Error = ""
	}); err != nil {
		return err
	}
	if isGIDNotFound(err) {
		m.enqueue(submission{id: id, removeResult: true})
	}
	return nil
}

func (m *Manager) DeleteTask(id int64) bool {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	task, ok := m.tasks[id]
	if !ok || task.Status == StatusQueued || task.Status == StatusDownloading || task.Status == StatusPaused {
		m.mu.Unlock()
		return false
	}
	if err := m.store.Delete(id); err != nil {
		log.Printf("delete task %d from database: %v", id, err)
		m.mu.Unlock()
		return false
	}
	delete(m.tasks, id)
	delete(m.fingerprints, task.Fingerprint)
	gid := task.GID
	m.mu.Unlock()
	if err := m.rpc.removeResult(gid); err != nil && !isGIDNotFound(err) {
		log.Printf("remove aria2 result %s: %v", gid, err)
	}
	return true
}

func (m *Manager) ClearDone() int {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	if err := m.store.ClearDone(); err != nil {
		log.Printf("clear completed records from database: %v", err)
		m.mu.Unlock()
		return 0
	}
	var gids []string
	for id, task := range m.tasks {
		if task.Status == StatusDone {
			gids = append(gids, task.GID)
			delete(m.fingerprints, task.Fingerprint)
			delete(m.tasks, id)
		}
	}
	m.mu.Unlock()
	for _, gid := range gids {
		if err := m.rpc.removeResult(gid); err != nil && !isGIDNotFound(err) {
			log.Printf("remove aria2 result %s: %v", gid, err)
		}
	}
	return len(gids)
}

func (m *Manager) enqueue(item submission) {
	m.pendingMu.Lock()
	m.pending = append(m.pending, item)
	m.pendingMu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) enqueueAdmission(item admission) {
	m.admissionMu.Lock()
	m.admissions = append(m.admissions, item)
	m.admissionMu.Unlock()
	select {
	case m.dbWake <- struct{}{}:
	default:
	}
}

func (m *Manager) persistenceWriter() {
	defer m.wg.Done()
	for {
		select {
		case <-m.done:
			m.flushAdmissions(true)
			return
		case <-m.dbWake:
			select {
			case <-m.done:
				m.flushAdmissions(true)
				return
			case <-time.After(2 * time.Millisecond):
			}
			m.flushAdmissions(false)
		}
	}
}

func (m *Manager) flushAdmissions(stopping bool) {
	for {
		m.admissionMu.Lock()
		batchSize := min(len(m.admissions), 256)
		if batchSize == 0 {
			m.admissionMu.Unlock()
			return
		}
		batch := append([]admission(nil), m.admissions[:batchSize]...)
		m.admissions = m.admissions[batchSize:]
		m.admissionMu.Unlock()

		tasks := make([]*Task, 0, len(batch))
		for _, item := range batch {
			tasks = append(tasks, item.task)
		}
		if err := m.store.InsertBatch(tasks); err != nil {
			log.Printf("persist %d admitted downloads: %v", len(batch), err)
			m.admissionMu.Lock()
			m.admissions = append(batch, m.admissions...)
			m.admissionMu.Unlock()
			if stopping {
				return
			}
			select {
			case <-m.done:
				stopping = true
			case <-time.After(time.Second):
			}
			continue
		}
		for _, item := range batch {
			m.enqueue(submission{id: item.task.ID})
		}
	}
}

func (m *Manager) dispatcher() {
	defer m.wg.Done()
	for {
		select {
		case <-m.done:
			return
		case <-m.wake:
		}
		for {
			m.pendingMu.Lock()
			if len(m.pending) == 0 {
				m.pendingMu.Unlock()
				break
			}
			item := m.pending[0]
			m.pending[0] = submission{}
			m.pending = m.pending[1:]
			m.pendingMu.Unlock()
			select {
			case <-m.done:
				return
			default:
				m.submit(item)
			}
		}
	}
}

func (m *Manager) submit(item submission) {
	m.mu.RLock()
	task, ok := m.tasks[item.id]
	if !ok {
		m.mu.RUnlock()
		return
	}
	snapshot := cloneTask(task)
	m.mu.RUnlock()
	if snapshot.Status != StatusQueued && snapshot.Status != StatusPaused {
		return
	}
	if err := os.MkdirAll(snapshot.Folder, 0755); err != nil {
		m.failTask(snapshot.ID, fmt.Errorf("create download directory: %w", err))
		return
	}
	if item.removeResult {
		if err := m.rpc.removeResult(snapshot.GID); err != nil && !isGIDNotFound(err) {
			log.Printf("remove stale aria2 result %s: %v", snapshot.GID, err)
		}
	}
	options := ariaOptions(snapshot, item.recheck)
	if err := m.rpc.addURI(snapshot, options); err != nil {
		m.failTask(snapshot.ID, err)
		return
	}
	_ = m.setTask(snapshot.ID, func(t *Task) {
		if t.Status == StatusPaused {
			t.Progress = "Paused by aria2"
		} else {
			t.Status = StatusQueued
			t.Progress = "Accepted by aria2"
		}
		t.Error = ""
	})
}

func (m *Manager) poller() {
	defer m.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			statuses, err := m.rpc.statuses()
			if err != nil {
				log.Printf("poll aria2 status: %v", err)
				continue
			}
			m.applyStatuses(statuses)
		}
	}
}

func (m *Manager) applyStatuses(statuses []ariaStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byGID := make(map[string]*Task, len(m.tasks))
	for _, task := range m.tasks {
		byGID[task.GID] = task
	}
	for _, state := range statuses {
		task := byGID[state.GID]
		if task == nil {
			continue
		}
		oldStatus, oldProgress, oldError, oldOutput := task.Status, task.Progress, task.Error, task.OutputName
		task.Status = statusFromAria(state.Status)
		task.Progress = formatProgress(state)
		task.Error = ""
		if task.Status == StatusError {
			task.Error = state.ErrorMessage
			if task.Error == "" {
				task.Error = "aria2 error code " + state.ErrorCode
			}
		}
		if task.OutputName == "" && len(state.Files) > 0 && state.Files[0].Path != "" {
			task.OutputName = filepath.Base(state.Files[0].Path)
		}
		if oldStatus != task.Status || oldProgress != task.Progress || oldError != task.Error || oldOutput != task.OutputName {
			task.UpdatedAt = time.Now()
			if err := m.store.Update(task); err != nil {
				log.Printf("persist task %d status: %v", task.ID, err)
			}
		}
	}
}

func (m *Manager) setTask(id int64, update func(*Task)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return fmt.Errorf("task %d not found", id)
	}
	update(task)
	task.UpdatedAt = time.Now()
	return m.store.Update(task)
}

func (m *Manager) failTask(id int64, err error) {
	if persistErr := m.setTask(id, func(t *Task) {
		t.Status = StatusError
		t.Error = err.Error()
		t.Progress = ""
	}); persistErr != nil {
		log.Printf("persist failed task %d: %v", id, persistErr)
	}
}

func (m *Manager) startAria2() error {
	if _, err := os.Stat(m.aria2Path); err != nil {
		return fmt.Errorf("aria2 executable: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("reserve aria2 RPC port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	secretBytes := make([]byte, 16)
	if _, err := rand.Read(secretBytes); err != nil {
		return err
	}
	secret := hex.EncodeToString(secretBytes)
	logPath := filepath.Join(filepath.Dir(m.defaultDir), "aria2.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open aria2 log: %w", err)
	}
	args := []string{
		"--enable-rpc=true",
		"--rpc-listen-all=false",
		fmt.Sprintf("--rpc-listen-port=%d", port),
		"--rpc-secret=" + secret,
		"--dir=" + filepath.ToSlash(m.defaultDir),
		"--continue=true",
		"--max-concurrent-downloads=3",
		"--console-log-level=warn",
		"--summary-interval=0",
		"--download-result=full",
		"--keep-unfinished-download-result=true",
		"--max-download-result=256",
	}
	cmd := exec.Command(m.aria2Path, args...)
	cmd.Dir = filepath.Dir(m.aria2Path)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start aria2 RPC service: %w", err)
	}
	m.cmd, m.logFile = cmd, logFile
	m.cmdDone = make(chan error, 1)
	go func() { m.cmdDone <- cmd.Wait() }()
	m.rpc = newAriaClient(port, secret)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if err := m.rpc.ready(); err == nil {
			return nil
		}
		select {
		case err := <-m.cmdDone:
			_ = logFile.Close()
			return fmt.Errorf("aria2 exited during startup: %w", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = cmd.Process.Kill()
	_ = logFile.Close()
	return fmt.Errorf("aria2 RPC service did not become ready")
}

func normalizeRequest(link, name, folder, defaultDir string, headers map[string]string, downloadPage string, queueID int, opts Aria2Opts) requestIdentity {
	if folder == "" {
		folder = defaultDir
	}
	cleanHeaders := make(map[string]string, len(headers))
	for key, value := range headers {
		cleanHeaders[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	if opts.ExtraArgs == nil {
		opts.ExtraArgs = []string{}
	}
	return requestIdentity{
		Link:         strings.TrimSpace(link),
		Name:         strings.TrimSpace(name),
		Folder:       filepath.Clean(strings.TrimSpace(folder)),
		QueueID:      queueID,
		Headers:      cleanHeaders,
		DownloadPage: strings.TrimSpace(downloadPage),
		Opts:         opts,
	}
}

func ariaOptions(task *Task, recheck bool) map[string]any {
	connections := task.Opts.Connections
	if connections <= 0 {
		connections = 16
	}
	maxTries := task.Opts.MaxTries
	if maxTries <= 0 {
		maxTries = 5
	}
	retryWait := task.Opts.RetryWait
	if retryWait <= 0 {
		retryWait = 3
	}
	options := map[string]any{
		"gid":                       task.GID,
		"dir":                       filepath.ToSlash(task.Folder),
		"continue":                  "true",
		"remote-time":               "true",
		"split":                     strconv.Itoa(connections),
		"max-connection-per-server": strconv.Itoa(connections),
		"max-tries":                 strconv.Itoa(maxTries),
		"retry-wait":                strconv.Itoa(retryWait),
		"pause":                     strconv.FormatBool(task.Status == StatusPaused),
	}
	if task.OutputName != "" {
		options["out"] = task.OutputName
		options["auto-file-renaming"] = "false"
	}
	if task.Opts.MaxSpeedBps > 0 {
		options["max-download-limit"] = strconv.Itoa(task.Opts.MaxSpeedBps)
	}
	if len(task.Headers) > 0 {
		keys := make([]string, 0, len(task.Headers))
		for key := range task.Headers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		headerLines := make([]string, 0, len(keys))
		for _, key := range keys {
			headerLines = append(headerLines, key+": "+task.Headers[key])
		}
		options["header"] = headerLines
	}
	if task.DownloadPage != "" {
		options["referer"] = task.DownloadPage
	}
	for _, raw := range task.Opts.ExtraArgs {
		value := strings.TrimSpace(strings.TrimPrefix(raw, "--"))
		if value == "" {
			continue
		}
		parts := strings.SplitN(value, "=", 2)
		optionValue := "true"
		if len(parts) == 2 {
			optionValue = parts[1]
		}
		if isProtectedAriaOption(parts[0]) {
			continue
		}
		options[parts[0]] = optionValue
	}
	if recheck {
		options["conditional-get"] = "true"
		options["allow-overwrite"] = "true"
		options["auto-file-renaming"] = "false"
	}
	return options
}

func isProtectedAriaOption(name string) bool {
	switch name {
	case "gid", "dir", "out", "pause", "continue", "conditional-get", "allow-overwrite", "auto-file-renaming":
		return true
	default:
		return false
	}
}

func statusFromAria(status string) Status {
	switch status {
	case "active":
		return StatusDownloading
	case "waiting":
		return StatusQueued
	case "paused":
		return StatusPaused
	case "complete":
		return StatusDone
	default:
		return StatusError
	}
}

func formatProgress(state ariaStatus) string {
	completed, _ := strconv.ParseInt(state.CompletedLength, 10, 64)
	total, _ := strconv.ParseInt(state.TotalLength, 10, 64)
	speed, _ := strconv.ParseInt(state.DownloadSpeed, 10, 64)
	if state.Status == "paused" {
		return fmt.Sprintf("Paused at %s / %s", formatBytes(completed), formatBytes(total))
	}
	if total <= 0 {
		return fmt.Sprintf("%s downloaded, %s/s", formatBytes(completed), formatBytes(speed))
	}
	return fmt.Sprintf("%.1f%% (%s / %s), %s/s", float64(completed)*100/float64(total), formatBytes(completed), formatBytes(total), formatBytes(speed))
}

func formatBytes(value int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	n := float64(value)
	unit := 0
	for n >= 1024 && unit < len(units)-1 {
		n /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", value, units[unit])
	}
	return fmt.Sprintf("%.1f %s", n, units[unit])
}

func newGID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

func displayName(link string) string {
	parsed, err := url.Parse(link)
	if err == nil {
		name, unescapeErr := url.PathUnescape(filepath.Base(parsed.Path))
		if unescapeErr == nil && name != "" && name != "." && name != "/" {
			return name
		}
	}
	return "download"
}

func cloneTask(task *Task) *Task {
	clone := *task
	clone.Headers = make(map[string]string, len(task.Headers))
	for key, value := range task.Headers {
		clone.Headers[key] = value
	}
	clone.Opts.ExtraArgs = append([]string(nil), task.Opts.ExtraArgs...)
	return &clone
}

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
	for _, path := range []string{filepath.Join(dir, name), filepath.Join(dir, name+".aria2")} {
		if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
			return false
		}
	}
	return true
}
