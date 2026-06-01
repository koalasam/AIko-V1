"""
server.py -- WebUI Hub Server
Central WebSocket hub that:
  - Receives chat messages from chat_interface.py  (role: "chat")
  - Forwards messages to llm_interface.py          (role: "llm")
  - Broadcasts everything to browser clients       (role: "ui")
  - Accepts manual messages typed in the UI

All configuration is loaded from config.env in the project root or same directory.

Usage:
    python server.py
    Then open http://localhost:8080 in your browser.

The chat_interface.py and llm_interface.py must be updated to point at this
server's WS_HUB_PORT instead of talking directly to each other.
"""

import asyncio
import json
import os
import uuid
from collections import deque
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
for env_path in ["../config.env", "config.env", "../llm/config.env"]:
    if Path(env_path).exists():
        load_dotenv(env_path)
        break

WS_HUB_HOST       = os.getenv("WS_HUB_HOST", "localhost")
WS_HUB_PORT       = int(os.getenv("WS_HUB_PORT", "8765"))
UI_HOST           = os.getenv("UI_HOST", "0.0.0.0")
UI_PORT           = int(os.getenv("UI_PORT", "8080"))
DEFAULT_CONCURRENCY = int(os.getenv("DEFAULT_CONCURRENCY", "1"))

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Stream Chat LLM Hub")
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


# ---------------------------------------------------------------------------
# Hub state
# ---------------------------------------------------------------------------
class Hub:
    def __init__(self):
        # Connected clients by role
        self.ui_clients:   set[WebSocket] = set()
        self.llm_client:   WebSocket | None = None
        self.chat_client:  WebSocket | None = None

        # Message queue: deque of message dicts (with metadata)
        self.queue: deque[dict] = deque()

        # Settings
        self.concurrency:    int  = DEFAULT_CONCURRENCY
        self.priority_mode:  bool = True   # True = user-priority, False = FIFO

        # Track how many LLM responses are currently in-flight
        self._in_flight: int = 0
        self._queue_event = asyncio.Event()

        # All messages ever (for UI reconnect replay, capped at 500)
        self.chat_log:     deque[dict] = deque(maxlen=500)
        self.response_log: deque[dict] = deque(maxlen=500)

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------

    async def broadcast_ui(self, payload: dict):
        """Send a JSON payload to all connected browser clients."""
        dead = set()
        msg = json.dumps(payload)
        for ws in self.ui_clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)
        self.ui_clients -= dead

    async def send_to_llm(self, payload: dict):
        """Forward a message to the connected LLM worker."""
        if self.llm_client:
            try:
                await self.llm_client.send_text(json.dumps(payload))
                return True
            except Exception:
                self.llm_client = None
                await self.broadcast_ui({"type": "status", "llm_connected": False})
        return False

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def enqueue(self, msg: dict):
        """Add a message to the processing queue."""
        if self.priority_mode and msg.get("source") == "ui":
            # UI messages jump to the front
            self.queue.appendleft(msg)
        else:
            self.queue.append(msg)
        self._queue_event.set()

    async def dispatcher(self):
        """
        Background task: drain queue respecting concurrency limit.
        Wakes up whenever a new item is enqueued or a response returns.
        """
        while True:
            await self._queue_event.wait()
            self._queue_event.clear()

            while self.queue and self._in_flight < self.concurrency:
                msg = self.queue.popleft()
                self._in_flight += 1
                await self.broadcast_ui({
                    "type": "queue_update",
                    "queue_length": len(self.queue),
                    "in_flight": self._in_flight,
                })
                success = await self.send_to_llm(msg)
                if not success:
                    # LLM not connected — put it back and wait
                    self.queue.appendleft(msg)
                    self._in_flight -= 1
                    break

            await self.broadcast_ui({
                "type": "queue_update",
                "queue_length": len(self.queue),
                "in_flight": self._in_flight,
            })

    def response_received(self):
        """Called when LLM returns a response; decrements in-flight counter."""
        self._in_flight = max(0, self._in_flight - 1)
        self._queue_event.set()   # wake dispatcher


hub = Hub()


# ---------------------------------------------------------------------------
# HTTP status endpoint
# ---------------------------------------------------------------------------
@app.get("/api/status")
async def status():
    return {
        "llm_connected":  hub.llm_client is not None,
        "chat_connected": hub.chat_client is not None,
        "ui_clients":     len(hub.ui_clients),
        "queue_length":   len(hub.queue),
        "in_flight":      hub._in_flight,
        "concurrency":    hub.concurrency,
        "priority_mode":  hub.priority_mode,
    }


# ---------------------------------------------------------------------------
# WebSocket: browser UI
# ---------------------------------------------------------------------------
@app.websocket("/ws/ui")
async def ws_ui(websocket: WebSocket):
    await websocket.accept()
    hub.ui_clients.add(websocket)

    # Send current state on connect
    await websocket.send_text(json.dumps({
        "type": "init",
        "concurrency":   hub.concurrency,
        "priority_mode": hub.priority_mode,
        "queue_length":  len(hub.queue),
        "in_flight":     hub._in_flight,
        "llm_connected":  hub.llm_client is not None,
        "chat_connected": hub.chat_client is not None,
        "chat_log":      list(hub.chat_log),
        "response_log":  list(hub.response_log),
    }))

    try:
        async for raw in websocket.iter_text():
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = data.get("action")

            # -- Settings changes --
            if action == "set_concurrency":
                hub.concurrency = max(1, int(data.get("value", 1)))
                await hub.broadcast_ui({"type": "settings", "concurrency": hub.concurrency})
                hub._queue_event.set()

            elif action == "set_priority_mode":
                hub.priority_mode = bool(data.get("value", True))
                await hub.broadcast_ui({"type": "settings", "priority_mode": hub.priority_mode})

            # -- Manual message from UI --
            elif action == "send_message":
                text = data.get("message", "").strip()
                if not text:
                    continue
                msg_id = str(uuid.uuid4())[:8]
                msg = {
                    "id":       msg_id,
                    "platform": "ui",
                    "username": data.get("username", "Moderator"),
                    "message":  text,
                    "source":   "ui",
                }
                hub.chat_log.append(msg)
                await hub.broadcast_ui({"type": "chat_message", **msg})
                hub.enqueue(msg)

            # -- Flag a message --
            elif action == "flag_message":
                payload = {
                    "type":    "flag",
                    "msg_id":  data.get("msg_id"),
                    "kind":    data.get("kind", "chat"),  # "chat" | "response"
                    "reason":  data.get("reason", ""),
                }
                await hub.broadcast_ui(payload)

            # -- Clear queue --
            elif action == "clear_queue":
                hub.queue.clear()
                await hub.broadcast_ui({
                    "type": "queue_update",
                    "queue_length": 0,
                    "in_flight": hub._in_flight,
                })

    except WebSocketDisconnect:
        pass
    finally:
        hub.ui_clients.discard(websocket)


# ---------------------------------------------------------------------------
# WebSocket: chat_interface.py
# ---------------------------------------------------------------------------
@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    await websocket.accept()
    hub.chat_client = websocket
    print("[Hub] chat_interface connected.")
    await hub.broadcast_ui({"type": "status", "chat_connected": True})

    try:
        async for raw in websocket.iter_text():
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[Hub] Bad JSON from chat: {raw!r}")
                continue

            msg_id = str(uuid.uuid4())[:8]
            msg = {
                "id":       msg_id,
                "platform": data.get("platform", "unknown"),
                "username": data.get("username", "unknown"),
                "message":  data.get("message", ""),
                "source":   "chat",
            }

            hub.chat_log.append(msg)
            await hub.broadcast_ui({"type": "chat_message", **msg})
            hub.enqueue(msg)

    except WebSocketDisconnect:
        pass
    finally:
        hub.chat_client = None
        print("[Hub] chat_interface disconnected.")
        await hub.broadcast_ui({"type": "status", "chat_connected": False})


# ---------------------------------------------------------------------------
# WebSocket: llm_interface.py
# ---------------------------------------------------------------------------
@app.websocket("/ws/llm")
async def ws_llm(websocket: WebSocket):
    await websocket.accept()
    hub.llm_client = websocket
    print("[Hub] llm_interface connected.")
    await hub.broadcast_ui({"type": "status", "llm_connected": True})
    hub._queue_event.set()  # flush anything waiting

    try:
        async for raw in websocket.iter_text():
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[Hub] Bad JSON from LLM: {raw!r}")
                hub.response_received()
                continue

            resp_id = str(uuid.uuid4())[:8]
            resp = {
                "id":           resp_id,
                "ref_id":       data.get("ref_id", ""),
                "platform":     data.get("platform", ""),
                "username":     data.get("username", ""),
                "orig_message": data.get("orig_message", ""),
                "response":     data.get("response", ""),
            }
            hub.response_log.append(resp)
            await hub.broadcast_ui({"type": "llm_response", **resp})
            hub.response_received()

    except WebSocketDisconnect:
        pass
    finally:
        hub.llm_client = None
        print("[Hub] llm_interface disconnected.")
        await hub.broadcast_ui({"type": "status", "llm_connected": False})


# ---------------------------------------------------------------------------
# Startup: launch dispatcher background task
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    asyncio.create_task(hub.dispatcher())
    print(f"[Hub] Server running — UI at http://{UI_HOST}:{UI_PORT}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host=UI_HOST,
        port=UI_PORT,
        reload=False,
        log_level="info",
    )