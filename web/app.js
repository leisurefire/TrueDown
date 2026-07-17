const POLL_INTERVAL_MS = 4000;
const LEGACY_THEME_KEY = "truedown-theme";

const statusMeta = {
  downloading: { label: "下载中", rank: 0 },
  queued: { label: "排队中", rank: 1 },
  paused: { label: "已暂停", rank: 2 },
  done: { label: "已完成", rank: 3 },
  error: { label: "出错", rank: 4 },
};

const els = {};
let toastTimer = 0;
let modalMode = "single";
let currentTasks = [];

document.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  initTheme();
  bindEvents();
  loadTasks();
  setInterval(loadTasks, POLL_INTERVAL_MS);
});

function cacheElements() {
  [
    "active-count",
    "batch-task-btn",
    "cfg-conns",
    "cfg-extra",
    "cfg-speed",
    "cfg-tries",
    "cfg-wait",
    "clear-done-btn",
    "download-form",
    "error-count",
    "m-conns",
    "m-extra",
    "m-folder",
    "m-headers",
    "m-link",
    "m-name",
    "m-queueid",
    "m-referer",
    "m-speed",
    "m-tries",
    "m-wait",
    "modal-cancel-btn",
    "modal-close-btn",
    "modal-eyebrow",
    "modal-msg",
    "modal-title",
    "new-task-btn",
    "overlay",
    "refresh-tasks-btn",
    "retry-all-btn",
    "submit-task-btn",
    "task-count",
    "tasks-container",
    "toast",
  ].forEach((id) => {
    els[toCamel(id)] = document.getElementById(id);
  });
}

function bindEvents() {
  els.newTaskBtn.addEventListener("click", () => openModal("single"));
  els.batchTaskBtn.addEventListener("click", () => openModal("batch"));
  els.modalCloseBtn.addEventListener("click", closeModal);
  els.modalCancelBtn.addEventListener("click", closeModal);
  els.overlay.addEventListener("click", (event) => {
    if (event.target === els.overlay) closeModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeModal();
  });

  els.downloadForm.addEventListener("submit", submitTask);
  els.refreshTasksBtn.addEventListener("click", () => loadTasks(true));
  els.retryAllBtn.addEventListener("click", requeueAllErrorTasks);
  els.clearDoneBtn.addEventListener("click", clearDone);
  els.tasksContainer.addEventListener("click", onTaskAction);
}

function initTheme() {
  localStorage.removeItem(LEGACY_THEME_KEY);
  document.documentElement.removeAttribute("data-theme");
}

function openModal(mode = "single") {
  modalMode = mode;
  const isBatch = mode === "batch";
  els.modalEyebrow.textContent = isBatch ? "Batch download" : "New download";
  els.modalTitle.textContent = isBatch ? "批量下载任务" : "新建下载任务";
  els.submitTaskBtn.textContent = isBatch ? "批量开始" : "开始下载";
  els.mLink.rows = isBatch ? 7 : 4;
  els.mLink.placeholder = isBatch
    ? "https://example.com/file-a.zip\nhttps://example.com/file-b.zip\nhttps://example.com/file-c.zip"
    : "https://example.com/file.zip";
  els.mName.disabled = isBatch;
  els.mName.placeholder = isBatch ? "批量时自动命名" : "留空自动命名";
  if (isBatch) els.mName.value = "";
  els.overlay.classList.add("open");
  els.overlay.setAttribute("aria-hidden", "false");
  window.setTimeout(() => els.mLink.focus(), 80);
}

function closeModal() {
  els.overlay.classList.remove("open");
  els.overlay.setAttribute("aria-hidden", "true");
  showModalMsg("");
}

async function submitTask(event) {
  event.preventDefault();
  const links = parseLinks(els.mLink.value);
  if (!links.length) {
    showModalMsg("请填写下载链接", true);
    return;
  }

  let headers = {};
  const rawHeaders = els.mHeaders.value.trim();
  if (rawHeaders) {
    try {
      headers = JSON.parse(rawHeaders);
    } catch {
      showModalMsg("Headers JSON 格式错误", true);
      return;
    }
  }

  const sharedBody = {
    headers,
    downloadPage: emptyToUndefined(els.mReferer.value),
    folder: emptyToUndefined(els.mFolder.value),
    name: links.length === 1 ? emptyToUndefined(els.mName.value) : undefined,
    queueId: optionalInt("mQueueid") || undefined,
    opts: buildOpts("m"),
  };

  setSubmitting(true);
  try {
    const created = await mapLimit(links, 8, (link) =>
      requestText("/start-headless-download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(buildStartBody(link, sharedBody)),
      }),
    );

    const duplicateCount = created.filter((text) => text.includes("DUPLICATE")).length;
    const summary = duplicateCount
      ? `已接收 ${created.length} 项，其中 ${duplicateCount} 项复用原记录并检查更新`
      : `已创建 ${created.length} 个任务`;
    showModalMsg(links.length === 1 ? (duplicateCount ? "已复用原记录并检查更新" : `已创建：${created[0]}`) : summary);
    els.mLink.value = "";
    await loadTasks();
    window.setTimeout(closeModal, 700);
  } catch (error) {
    showModalMsg(`创建失败：${error.message}`, true);
  } finally {
    setSubmitting(false);
  }
}

function setSubmitting(isSubmitting) {
  els.submitTaskBtn.disabled = isSubmitting;
  if (isSubmitting) {
    els.submitTaskBtn.textContent = "提交中...";
    return;
  }
  els.submitTaskBtn.textContent = modalMode === "batch" ? "批量开始" : "开始下载";
}

function buildStartBody(link, sharedBody) {
  return {
    downloadSource: {
      link,
      headers: sharedBody.headers,
      downloadPage: sharedBody.downloadPage,
    },
    folder: sharedBody.folder,
    name: sharedBody.name,
    queueId: sharedBody.queueId,
    opts: sharedBody.opts,
  };
}

function buildOpts(prefix) {
  const extraFromModal = lines(`${prefix}Extra`);
  return {
    connections: optionalInt(`${prefix}Conns`) || optionalInt("cfgConns"),
    maxSpeedBps: optionalInt(`${prefix}Speed`) || optionalInt("cfgSpeed"),
    maxTries: optionalInt(`${prefix}Tries`) || optionalInt("cfgTries"),
    retryWait: optionalInt(`${prefix}Wait`) || optionalInt("cfgWait"),
    extraArgs: extraFromModal.length ? extraFromModal : lines("cfgExtra"),
  };
}

async function onTaskAction(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const id = button.dataset.id;
  const action = button.dataset.action;
  if (action === "requeue") {
    await requeueTask(id);
  }
  if (action === "pause") {
    await changePauseState(id, true);
  }
  if (action === "resume") {
    await changePauseState(id, false);
  }
  if (action === "copy-link") {
    await copyTaskLink(button.dataset.link || "");
  }
  if (action === "delete") {
    await deleteTask(id);
  }
}

async function requeueTask(id) {
  try {
    await requestText(`/tasks/requeue?id=${encodeURIComponent(id)}`, { method: "POST" });
    showToast("已交给 aria2 继续下载，已有进度会保留。");
    await loadTasks();
  } catch (error) {
    showToast(`重试失败：${error.message}`, "error");
  }
}

async function changePauseState(id, pause) {
  try {
    await requestText(`/tasks/${pause ? "pause" : "resume"}?id=${encodeURIComponent(id)}`, { method: "POST" });
    showToast(pause ? "任务已由 aria2 暂停。" : "任务已由 aria2 恢复。");
    await loadTasks();
  } catch (error) {
    showToast(`${pause ? "暂停" : "恢复"}失败：${error.message}`, "error");
  }
}

async function requeueAllErrorTasks() {
  const errorTasks = currentTasks.filter((task) => task.status === "error");
  if (!errorTasks.length) {
    showToast("没有需要重试的任务。");
    return;
  }

  const oldText = els.retryAllBtn.textContent;
  els.retryAllBtn.disabled = true;
  els.retryAllBtn.textContent = "重试中...";

  let succeeded = 0;
  let firstError = "";
  for (const task of errorTasks) {
    try {
      await requestText(`/tasks/requeue?id=${encodeURIComponent(task.id)}`, { method: "POST" });
      succeeded += 1;
    } catch (error) {
      firstError ||= error.message;
    }
  }

  els.retryAllBtn.textContent = oldText;
  await loadTasks();
  els.retryAllBtn.disabled = currentTasks.every((task) => task.status !== "error");

  if (succeeded === errorTasks.length) {
    showToast(`已重新排队 ${succeeded} 个失败任务。`);
    return;
  }
  showToast(`已重试 ${succeeded}/${errorTasks.length} 个任务：${firstError || "部分失败"}`, "error");
}

async function deleteTask(id) {
  try {
    await requestText(`/tasks/delete?id=${encodeURIComponent(id)}`, { method: "POST" });
    showToast("任务记录已删除。");
    await loadTasks();
  } catch (error) {
    showToast(`删除失败：${error.message}`, "error");
  }
}

async function clearDone() {
  try {
    const text = await requestText("/tasks/clear-done", { method: "POST" });
    showToast(text.replace("OK", "已清理"));
    await loadTasks();
  } catch (error) {
    showToast(`清理失败：${error.message}`, "error");
  }
}

async function loadTasks(showSuccess = false) {
  try {
    const response = await fetch("/tasks");
    if (!response.ok) {
      throw new Error(await response.text());
    }
    const tasks = await response.json();
    renderTasks(Array.isArray(tasks) ? tasks : []);
    if (showSuccess) showToast("任务列表已刷新。");
  } catch (error) {
    console.error("loadTasks:", error);
    showToast(`加载任务失败：${error.message}`, "error");
  }
}

function renderTasks(tasks) {
  currentTasks = tasks;
  updateMetrics(tasks);

  if (!tasks.length) {
    els.tasksContainer.innerHTML = emptyMarkup();
    return;
  }

  const sortedTasks = [...tasks].sort((a, b) => {
    const statusDiff = (statusMeta[a.status]?.rank ?? 9) - (statusMeta[b.status]?.rank ?? 9);
    return statusDiff || b.id - a.id;
  });

  const rows = sortedTasks.map((task, index) => taskRow(task, index)).join("");
  els.tasksContainer.innerHTML = `
    <table class="tasks-table">
      <colgroup>
        <col class="col-index">
        <col class="col-file">
        <col class="col-status">
        <col class="col-link">
        <col class="col-progress">
        <col class="col-actions">
      </colgroup>
      <thead>
        <tr>
          <th scope="col">#</th>
          <th scope="col">文件</th>
          <th scope="col">状态</th>
          <th scope="col">链接</th>
          <th scope="col">进度 / 日志</th>
          <th scope="col" class="align-right">操作</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function taskRow(task, index) {
  const status = task.status || "queued";
  const statusLabel = statusMeta[status]?.label || status;
  const progress = task.error ? `! ${task.error}` : task.progress || "-";
  const fileName = task.outputName || task.name || `任务 #${task.id}`;
  const actions = [];

  if (status === "error") {
    actions.push(`<button class="text-button" type="button" data-action="requeue" data-id="${task.id}">重试</button>`);
  }
  if (status === "queued" || status === "downloading") {
    actions.push(`<button class="text-button" type="button" data-action="pause" data-id="${task.id}">暂停</button>`);
  }
  if (status === "paused") {
    actions.push(`<button class="text-button" type="button" data-action="resume" data-id="${task.id}">继续</button>`);
  }
  if (status === "done" || status === "error") {
    actions.push(`<button class="text-button text-button-danger" type="button" data-action="delete" data-id="${task.id}">删除</button>`);
  }

  return `
    <tr>
      <td class="task-index">${index + 1}</td>
      <td>
        <div class="task-name" title="${esc(fileName)}">${esc(fileName)}</div>
        <div class="task-folder" title="${esc(task.folder || "默认目录")}">${esc(task.folder || "默认目录")}</div>
      </td>
      <td><span class="status-badge status-${esc(status)}">${esc(statusLabel)}</span></td>
      <td>
        <button class="task-link" type="button" data-action="copy-link" data-link="${esc(task.link)}" title="点击复制：${esc(task.link)}">${esc(compactUrl(task.link))}</button>
      </td>
      <td><div class="progress-line" title="${esc(progress)}">${esc(progress)}</div></td>
      <td><div class="row-actions">${actions.join("") || "—"}</div></td>
    </tr>
  `;
}

function updateMetrics(tasks) {
  const errorCount = tasks.filter((task) => task.status === "error").length;
  els.taskCount.textContent = tasks.length;
  els.activeCount.textContent = tasks.filter((task) => task.status === "queued" || task.status === "downloading").length;
  els.errorCount.textContent = errorCount;
  els.retryAllBtn.disabled = errorCount === 0;
}

function emptyMarkup() {
  return `
    <div class="empty-state">
      <div class="empty-icon" aria-hidden="true">↓</div>
      <h2>暂无任务</h2>
      <p>点击右上角「新建下载」开始添加链接。</p>
    </div>
  `;
}

async function requestText(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || response.statusText);
  }
  return text;
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;
  async function run() {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await worker(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return results;
}

async function copyTaskLink(link) {
  if (!link) {
    showToast("没有可复制的下载链接。", "error");
    return;
  }
  try {
    await writeClipboard(link);
    showToast("下载链接已复制。");
  } catch (error) {
    showToast(`复制失败：${error.message}`, "error");
  }
}

async function writeClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  textarea.remove();
  if (!copied) throw new Error("浏览器拒绝访问剪贴板");
}

function showModalMsg(text, isError = false) {
  els.modalMsg.textContent = text;
  els.modalMsg.classList.toggle("err", isError);
}

function showToast(text, type = "success") {
  window.clearTimeout(toastTimer);
  els.toast.textContent = text;
  els.toast.className = `toast show${type === "error" ? " error" : ""}`;
  toastTimer = window.setTimeout(() => {
    els.toast.className = "toast";
  }, 2600);
}

function optionalInt(key) {
  const value = els[key].value.trim();
  if (!value) return 0;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function lines(key) {
  return els[key].value
    .trim()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function parseLinks(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function emptyToUndefined(value) {
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function compactUrl(value) {
  if (!value) return "—";
  try {
    const url = new URL(value);
    return `${url.hostname}${url.pathname}`;
  } catch {
    return value;
  }
}

function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function toCamel(value) {
  return value.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
}
