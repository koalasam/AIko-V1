"""
audiostreamer.py — Audio Streamer Server
Runs a lightweight WebSocket server on port 8081.
Accepts audio file paths from tts_interface.py, queues them,
and plays them one at a time using ffplay (no UI needed).

No browser UI — this is a headless audio playback daemon.
"""

import asyncio
import json
import os
from pathlib import Path

import websockets
from dotenv import load_dotenv

for env_path in ["config.env", "../config.env"]:
    if Path(env_path).exists():
        load_dotenv(env_path)
        break

AUDIO_HUB_HOST = os.getenv("AUDIO_HUB_HOST", "localhost")
AUDIO_HUB_PORT = int(os.getenv("AUDIO_HUB_PORT", "8081"))
AUDIO_VOLUME   = float(os.getenv("AUDIO_VOLUME", "1.0"))   # 0.0–1.0

# Optional: notify the main hub about playback status
WS_HUB_HOST  = os.getenv("WS_HUB_HOST", "localhost")
WS_HUB_PORT  = int(os.getenv("WS_HUB_PORT", "8765"))
WS_STATUS_URI = f"ws://{WS_HUB_HOST}:{WS_HUB_PORT}/ws/audiostatus"


class AudioStreamer:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.playing: bool = False
        self.current_file: str = ""
        self.current_proc: asyncio.subprocess.Process | None = None
        self.paused: bool = False
        self.skip_requested: bool = False
        self.queue_items: list[dict] = []   # shadow list for status reporting

    # ------------------------------------------------------------------
    # Playback
    # ------------------------------------------------------------------

    async def player_loop(self):
        """Drain the queue and play each file in order."""
        while True:
            item = await self.queue.get()
            self.queue_items = self.queue_items[1:] if self.queue_items else []

            audio_path = item.get("audio_path", "")
            if not audio_path or not Path(audio_path).exists():
                print(f"[AudioStreamer] File not found: {audio_path!r}. Skipping.")
                self.queue.task_done()
                continue

            self.playing = True
            self.current_file = audio_path
            self.skip_requested = False

            vol = max(0.0, min(1.0, AUDIO_VOLUME))
            ffplay_vol = int(vol * 100)

            print(f"[AudioStreamer] Playing: {audio_path}")
            try:
                self.current_proc = await asyncio.create_subprocess_exec(
                    "ffplay",
                    "-nodisp",
                    "-autoexit",
                    "-volume", str(ffplay_vol),
                    audio_path,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await self.current_proc.wait()
            except FileNotFoundError:
                print("[AudioStreamer] ffplay not found. Install ffmpeg to enable playback.")
            except Exception as e:
                print(f"[AudioStreamer] Playback error: {e}")
            finally:
                self.playing = False
                self.current_file = ""
                self.current_proc = None
                self.queue.task_done()
                print("[AudioStreamer] Finished playing.")

    def skip(self):
        """Kill current playback."""
        if self.current_proc and self.playing:
            self.current_proc.terminate()
            self.skip_requested = True
            print("[AudioStreamer] Skipping current audio.")

    def clear_queue(self):
        """Empty the pending queue."""
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
        self.queue_items.clear()
        print("[AudioStreamer] Queue cleared.")

    # ------------------------------------------------------------------
    # WebSocket server: receives audio paths from tts_interface
    # ------------------------------------------------------------------

    async def ws_handler(self, websocket):
        """Handle an incoming connection from tts_interface or a control client."""
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[AudioStreamer] Bad JSON: {raw!r}")
                continue

            action = data.get("action")

            if action == "skip":
                self.skip()
            elif action == "clear":
                self.clear_queue()
            elif action == "set_volume":
                global AUDIO_VOLUME
                AUDIO_VOLUME = max(0.0, min(1.0, float(data.get("value", AUDIO_VOLUME))))
                print(f"[AudioStreamer] Volume set to {AUDIO_VOLUME:.2f}")
            elif action == "status":
                status = {
                    "playing":      self.playing,
                    "current_file": self.current_file,
                    "queue_length": self.queue.qsize(),
                    "volume":       AUDIO_VOLUME,
                }
                try:
                    await websocket.send(json.dumps(status))
                except Exception:
                    pass
            elif "audio_path" in data:
                # New audio file to play
                self.queue_items.append(data)
                await self.queue.put(data)
                print(f"[AudioStreamer] Queued: {data['audio_path']} "
                      f"(queue depth: {self.queue.qsize()})")
            else:
                print(f"[AudioStreamer] Unknown message: {data}")

    # ------------------------------------------------------------------
    # Optional: push status updates back to main hub
    # ------------------------------------------------------------------

    async def status_reporter(self):
        """Periodically try to connect to the main hub and send status."""
        while True:
            try:
                async with websockets.connect(WS_STATUS_URI) as ws:
                    print("[AudioStreamer] Status reporter connected to hub.")
                    while True:
                        status = {
                            "type":         "audiostatus",
                            "playing":      self.playing,
                            "current_file": self.current_file,
                            "queue_length": self.queue.qsize(),
                            "volume":       AUDIO_VOLUME,
                        }
                        await ws.send(json.dumps(status))
                        await asyncio.sleep(2)
            except Exception:
                pass  # Hub not up or route not available — just retry silently
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self):
        print(f"[AudioStreamer] Starting on {AUDIO_HUB_HOST}:{AUDIO_HUB_PORT}")

        server = await websockets.serve(
            self.ws_handler,
            AUDIO_HUB_HOST,
            AUDIO_HUB_PORT,
        )

        await asyncio.gather(
            server.wait_closed(),
            self.player_loop(),
            self.status_reporter(),
        )


if __name__ == "__main__":
    streamer = AudioStreamer()
    try:
        asyncio.run(streamer.run())
    except KeyboardInterrupt:
        print("\n[AudioStreamer] Stopped.")