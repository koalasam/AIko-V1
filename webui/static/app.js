/**
 * app.js — Stream Chat Hub frontend
 * Manages WebSocket connection to server.py and all UI interactions.
 */

"use strict";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const state = {
  ws: null,
  connected: false,
  concurrency: 1,
  priorityMode: true,
  queueLength: 0,
  inFlight: 0,
  flags: [],           // { id, kind, username, text, reason, ts }
  pendingFlag: null,   // { msgId, kind, text }
};

const MAX_QUEUE_VIZ = 20;  // queue bar maxes out visually at this

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const $ = id => document.getElementById(id);

const dom = {
  feedChat:       $("feed-chat"),
  feedResp:       $("feed-responses"),
  statusChat:     $("status-chat"),
  statusLLM:      $("status-llm"),
  statusWS:       $("status-ws"),
  hdrQueue:       $("hdr-queue"),
  hdrInFlight:    $("hdr-inflight"),
  btnPriority:    $("btn-priority"),
  btnFifo:        $("btn-fifo"),
  modeHint:       $("mode-hint"),
  sliderConc:     $("slider-concurrency"),
  concVal:        $("concurrency-val"),
  queueBar:       $("queue-bar"),
  statQueue:      $("stat-queue"),
  statInFlight:   $("stat-inflight"),
  btnClearQueue:  $("btn-clear-queue"),
  inputUsername:  $("input-username"),
  inputMessage:   $("input-message"),
  btnSend:        $("btn-send"),
  btnClearChat:   $("btn-clear-chat"),
  btnClearResp:   $("btn-clear-resp"),
  flagList:       $("flag-list"),
  flagCount:      $("flag-count"),
  btnClearFlags:  $("btn-clear-flags"),
  modalOverlay:   $("modal-overlay"),
  modalBody:      $("modal-body"),
  modalReason:    $("modal-reason"),
  modalConfirm:   $("modal-confirm"),
  modalCancel:    $("modal-cancel"),
};

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const uri = `${proto}://${location.host}/ws/ui`;

  setStatus(dom.statusWS, "connecting");

  const ws = new WebSocket(uri);
  state.ws = ws;

  ws.onopen = () => {
    state.connected = true;
    setStatus(dom.statusWS, "connected");
  };

  ws.onclose = () => {
    state.connected = false;
    setStatus(dom.statusWS, "disconnected");
    setTimeout(connect, 3000);
  };

  ws.onerror = () => {
    ws.close();
  };

  ws.onmessage = ev => {
    try {
      handleMessage(JSON.parse(ev.data));
    } catch (e) {
      console.warn("Bad WS message:", ev.data, e);
    }
  };
}

function send(payload) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(payload));
  }
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------
function handleMessage(msg) {
  switch (msg.type) {

    case "init":
      // Restore full state on (re)connect
      applySettings({
        concurrency:   msg.concurrency,
        priority_mode: msg.priority_mode,
      });
      updateQueueDisplay(msg.queue_length, msg.in_flight);
      setStatus(dom.statusChat, msg.chat_connected ? "connected" : "disconnected");
      setStatus(dom.statusLLM,  msg.llm_connected  ? "connected" : "disconnected");

      // Replay logs
      dom.feedChat.innerHTML = "";
      dom.feedResp.innerHTML = "";
      (msg.chat_log || []).forEach(m => appendChatCard(m));
      (msg.response_log || []).forEach(r => appendRespCard(r));
      break;

    case "chat_message":
      appendChatCard(msg);
      break;

    case "llm_response":
      appendRespCard(msg);
      break;

    case "status":
      if (msg.chat_connected !== undefined)
        setStatus(dom.statusChat, msg.chat_connected ? "connected" : "disconnected");
      if (msg.llm_connected !== undefined)
        setStatus(dom.statusLLM, msg.llm_connected ? "connected" : "disconnected");
      break;

    case "queue_update":
      updateQueueDisplay(msg.queue_length, msg.in_flight);
      break;

    case "settings":
      applySettings(msg);
      break;

    case "flag":
      // Flagged by another UI client — highlight the card
      markFlagged(msg.msg_id, msg.kind);
      break;
  }
}

// ---------------------------------------------------------------------------
// UI helpers
// ---------------------------------------------------------------------------
function setStatus(el, state) {
  el.className = `status-pill ${state}`;
}

function applySettings(s) {
  if (s.concurrency !== undefined) {
    state.concurrency = s.concurrency;
    dom.sliderConc.value = s.concurrency;
    dom.concVal.textContent = s.concurrency;
  }
  if (s.priority_mode !== undefined) {
    state.priorityMode = s.priority_mode;
    dom.btnPriority.classList.toggle("active", s.priority_mode);
    dom.btnFifo.classList.toggle("active", !s.priority_mode);
    dom.modeHint.textContent = s.priority_mode
      ? "UI messages jump the queue."
      : "All messages processed in order.";
  }
}

function updateQueueDisplay(qLen, inFlight) {
  state.queueLength = qLen;
  state.inFlight = inFlight;

  dom.hdrQueue.textContent    = qLen;
  dom.hdrInFlight.textContent = inFlight;
  dom.statQueue.textContent   = qLen;
  dom.statInFlight.textContent = inFlight;

  const pct = Math.min(100, (qLen / MAX_QUEUE_VIZ) * 100);
  dom.queueBar.style.width = pct + "%";
  dom.queueBar.style.background = pct > 80 ? "var(--red)" : pct > 50 ? "var(--amber)" : "var(--green)";
}

// ---------------------------------------------------------------------------
// Chat message card
// ---------------------------------------------------------------------------
function appendChatCard(msg) {
  const platform = (msg.platform || "unknown").toLowerCase();
  const card = document.createElement("div");
  card.className = `msg-card platform-${platform}`;
  card.dataset.msgId = msg.id || "";

  const time = formatTime();
  card.innerHTML = `
    <div class="msg-meta">
      <span class="msg-platform">${escHtml(platform)}</span>
      <span class="msg-username">${escHtml(msg.username || "?")}</span>
      <span class="msg-time">${time}</span>
    </div>
    <div class="msg-text">${escHtml(msg.message || "")}</div>
    <button class="msg-flag-btn" title="Flag this message">⚑ FLAG</button>
  `;

  card.querySelector(".msg-flag-btn").addEventListener("click", () => {
    openFlagModal(msg.id, "chat", `${msg.username}: ${msg.message}`);
  });

  dom.feedChat.appendChild(card);
  scrollToBottom(dom.feedChat);
}

// ---------------------------------------------------------------------------
// Response card
// ---------------------------------------------------------------------------
function appendRespCard(resp) {
  const card = document.createElement("div");
  card.className = "resp-card";
  card.dataset.msgId = resp.id || "";

  const ref = resp.username
    ? `<span>${escHtml(resp.username)}</span> on <span>${escHtml(resp.platform || "?")}</span>`
    : `ref <span>${escHtml(resp.ref_id || "?")}</span>`;

  card.innerHTML = `
    <div class="resp-ref">↳ reply to ${ref} · <span class="msg-time">${formatTime()}</span></div>
    ${resp.orig_message
      ? `<div class="msg-text" style="color:var(--text-dim);font-size:11px;margin-bottom:4px;">"${escHtml(resp.orig_message)}"</div>`
      : ""}
    <div class="resp-text">${escHtml(resp.response || "")}</div>
    <button class="msg-flag-btn" title="Flag this response">⚑ FLAG</button>
  `;

  card.querySelector(".msg-flag-btn").addEventListener("click", () => {
    openFlagModal(resp.id, "response", resp.response || "");
  });

  dom.feedResp.appendChild(card);
  scrollToBottom(dom.feedResp);
}

// ---------------------------------------------------------------------------
// Flag logic
// ---------------------------------------------------------------------------
function openFlagModal(msgId, kind, text) {
  state.pendingFlag = { msgId, kind, text };
  dom.modalBody.textContent = text.length > 120 ? text.slice(0, 120) + "…" : text;
  dom.modalReason.value = "";
  dom.modalOverlay.classList.remove("hidden");
  dom.modalReason.focus();
}

function closeFlagModal() {
  state.pendingFlag = null;
  dom.modalOverlay.classList.add("hidden");
}

function confirmFlag() {
  if (!state.pendingFlag) return;
  const { msgId, kind, text } = state.pendingFlag;
  const reason = dom.modalReason.value.trim();

  // Tell server (broadcasts to all UI clients)
  send({ action: "flag_message", msg_id: msgId, kind, reason });

  // Local flag record
  const flag = { id: msgId, kind, text, reason, ts: formatTime() };
  state.flags.push(flag);
  renderFlagList();

  // Mark the card locally
  markFlagged(msgId, kind);

  closeFlagModal();
}

function markFlagged(msgId, kind) {
  if (!msgId) return;
  const feed = kind === "response" ? dom.feedResp : dom.feedChat;
  const card = feed.querySelector(`[data-msg-id="${msgId}"]`);
  if (card) card.classList.add("flagged");
}

function renderFlagList() {
  dom.flagCount.textContent = state.flags.length;
  if (state.flags.length === 0) {
    dom.flagList.innerHTML = '<p class="empty-hint">No flags yet.</p>';
    return;
  }
  dom.flagList.innerHTML = state.flags.map(f => `
    <div class="flag-item">
      <div class="flag-kind">${f.kind.toUpperCase()} · ${f.ts}</div>
      <div>${escHtml(f.text.slice(0, 80))}${f.text.length > 80 ? "…" : ""}</div>
      ${f.reason ? `<div class="flag-reason">${escHtml(f.reason)}</div>` : ""}
    </div>
  `).reverse().join("");
}

// ---------------------------------------------------------------------------
// Manual message send
// ---------------------------------------------------------------------------
function sendManualMessage() {
  const username = dom.inputUsername.value.trim() || "Moderator";
  const message  = dom.inputMessage.value.trim();
  if (!message) return;

  send({ action: "send_message", username, message });
  dom.inputMessage.value = "";
  dom.inputMessage.focus();
}

// ---------------------------------------------------------------------------
// Event listeners
// ---------------------------------------------------------------------------

// Priority mode toggle
dom.btnPriority.addEventListener("click", () => {
  send({ action: "set_priority_mode", value: true });
});
dom.btnFifo.addEventListener("click", () => {
  send({ action: "set_priority_mode", value: false });
});

// Concurrency slider
dom.sliderConc.addEventListener("input", () => {
  const v = parseInt(dom.sliderConc.value, 10);
  dom.concVal.textContent = v;
});
dom.sliderConc.addEventListener("change", () => {
  send({ action: "set_concurrency", value: parseInt(dom.sliderConc.value, 10) });
});

// Clear queue
dom.btnClearQueue.addEventListener("click", () => {
  if (confirm("Clear the message queue?")) {
    send({ action: "clear_queue" });
  }
});

// Send button
dom.btnSend.addEventListener("click", sendManualMessage);
dom.inputMessage.addEventListener("keydown", e => {
  if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) sendManualMessage();
});

// Clear displays
dom.btnClearChat.addEventListener("click", () => { dom.feedChat.innerHTML = ""; });
dom.btnClearResp.addEventListener("click", () => { dom.feedResp.innerHTML = ""; });

// Clear flags
dom.btnClearFlags.addEventListener("click", () => {
  state.flags = [];
  renderFlagList();
});

// Modal
dom.modalConfirm.addEventListener("click", confirmFlag);
dom.modalCancel.addEventListener("click",  closeFlagModal);
dom.modalOverlay.addEventListener("click", e => {
  if (e.target === dom.modalOverlay) closeFlagModal();
});
document.addEventListener("keydown", e => {
  if (e.key === "Escape") closeFlagModal();
  if (e.key === "Enter" && !dom.modalOverlay.classList.contains("hidden")) confirmFlag();
});

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatTime() {
  const d = new Date();
  return d.toTimeString().slice(0, 8);
}

function scrollToBottom(el) {
  // Only auto-scroll if already near the bottom
  const threshold = 120;
  if (el.scrollHeight - el.scrollTop - el.clientHeight < threshold) {
    el.scrollTop = el.scrollHeight;
  }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
connect();