package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"truedown/internal/downloader"
)

type downloadSourceReq struct {
	Link         string            `json:"link"`
	Headers      map[string]string `json:"headers"`
	DownloadPage string            `json:"downloadPage"`
}

type startReq struct {
	DownloadSource downloadSourceReq    `json:"downloadSource"`
	Folder         string               `json:"folder"`
	Name           string               `json:"name"`
	QueueID        int                  `json:"queueId"`
	Opts           downloader.Aria2Opts `json:"opts"`
}

func Register(mux *http.ServeMux, dm *downloader.Manager) {
	mux.HandleFunc("/start-headless-download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req startReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.DownloadSource.Link == "" {
			http.Error(w, "downloadSource.link is required", http.StatusBadRequest)
			return
		}
		t := dm.AddTask(
			req.DownloadSource.Link,
			req.Name,
			req.Folder,
			req.DownloadSource.Headers,
			req.DownloadSource.DownloadPage,
			req.QueueID,
			req.Opts,
		)
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("OK " + strconv.FormatInt(t.ID, 10)))
	})

	mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		tasks := dm.ListTasks()
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].ID < tasks[j].ID
		})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tasks)
	})

	// POST /tasks/{id}/requeue — re-enqueue a failed task
	mux.HandleFunc("/tasks/requeue", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		if err := dm.RequeueTask(id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Write([]byte("OK"))
	})

	// POST /tasks/delete — delete a single done/error task by id
	mux.HandleFunc("/tasks/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		if !dm.DeleteTask(id) {
			http.Error(w, "task not found or still active", http.StatusBadRequest)
			return
		}
		w.Write([]byte("OK"))
	})

	// POST /tasks/clear-done — remove all completed tasks
	mux.HandleFunc("/tasks/clear-done", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		n := dm.ClearDone()
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("OK " + strconv.Itoa(n)))
	})
}
