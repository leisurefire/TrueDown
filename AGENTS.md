Purpose
-------
This document defines the development standards, modules, and collaboration
rules for the TrueDown project.
This repository contains a multi-queue download scheduler built on:
- Golang backend
- Aria2 RPC execution engine
- SQLite persistence
- Local-only REST API
- Material Design 3 WebUI

Architecture
------------
The project uses a three-layer structure:

1. Frontend WebUI
   - React + Vite + MUI (Material Design 3)
   - Local-only access (http://127.0.0.1:<port>)
   - Receives real-time updates via SSE (/api/v1/events)

2. Backend API layer (internal/api)
   - Exposes REST endpoints for addTask, import/export, queue control
   - Fire-and-forget design: accept requests and immediately ACK
   - Only accessible from localhost
   - Supports custom headers (User-Agent, Cookie, etc.) passed to Aria2

3. Scheduler + Aria2 integration
   - Three queues: image / video / other
   - Queue concurrency defined by scheduler:
       image=32, video=8, other=8
   - Tasks stored in SQLite
   - Scheduler enforces retry policies using exponential backoff
   - Aria2 is bundled in the project and started automatically by the server

Aria2 Integration
-----------------
Aria2 is bundled inside the release packages under /bin/.
At startup, the server verifies aria2c exists, extracts if missing,
and then starts aria2 with:
   --enable-rpc
   --rpc-listen-port=6800
   --rpc-listen-all=false
   --rpc-secret=<secret>
   --continue=true
Tasks are submitted via JSON-RPC (addUri, tellStatus, pause, etc.).

API Rules
---------
- All external communication uses HTTP REST on localhost.
- POST /api/v1/tasks/add receives:
    url, filename, headers, meta
- Handlers must:
    * Insert the task into SQLite
    * Return {"status":"ok"} immediately
    * Never block on scheduler or aria2
- Queue control via:
    POST /api/v1/queues/{type}/pause
    POST /api/v1/queues/{type}/resume
- Import/export endpoints must preserve full task metadata.

Scheduler Rules
---------------
- Scheduler pulls tasks from SQLite by queue and submits to Aria2 based on concurrency limits.
- Filetype classification:
    1. filename extension
    2. URL extension
    3. Aria2 metadata (tellStatus)
    4. HEAD / range 0-0 request if needed
- Retry strategy:
    initial=60s, max=1800s (30min)
    backoff: delay = min(initial * 2^retry, max)
- 404 stops retrying.
- Network errors, timeout, no-progress: auto pause + backoff.
- After backoff finishes, task is returned to queue head.

Persistence
-----------
SQLite database with tables:
    tasks(task_id, url, filename, filetype, queue_type, state, retries, headers, created_at, updated_at)
    history(...)
    queue_config(queue_type, enabled, max_concurrency)
History entries should be preserved unless manually cleaned by the user.

Development Rules
-----------------
- All modules reside under /internal/, except cmd/ and webui/.
- Backend must be stateless except SQLite.
- Scheduler must run as a dedicated goroutine.
- All Aria2 communication must be retried with bounded attempts.
- WebUI must not expose remote access.

Updating This File
------------------
Whenever any new feature affecting architecture, API, task scheduling, or
Aria2 integration is proposed, this file must be updated accordingly.