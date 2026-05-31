"""
chat_interface.py -- Chat collector + WebSocket client
Monitors YouTube and Twitch chat concurrently and streams all messages to
llm_interface.py via WebSocket.

All configuration is loaded from config.env (same directory).

Runs indefinitely until stopped with Ctrl+C.

Usage:
    python chat_interface.py
"""

import asyncio
import json
import os
import websockets
from dotenv import load_dotenv
from youtube_chat_conector import YouTubeChatMonitor
from twitch_chat_conector import TwitchChatMonitor

# -- Load config --------------------------------------------------------------
load_dotenv("config.env")

YOUTUBE_VIDEO_ID = os.getenv("YOUTUBE_VIDEO_ID", "")
TWITCH_CHANNEL   = os.getenv("TWITCH_CHANNEL", "")
WS_HOST          = os.getenv("WS_HOST", "localhost")
WS_PORT          = os.getenv("WS_PORT", "8765")
WS_URI           = f"ws://{WS_HOST}:{WS_PORT}"
RESTART_DELAY    = int(os.getenv("RESTART_DELAY", "5"))
ENABLE_YOUTUBE   = os.getenv("ENABLE_YOUTUBE", "true").lower() == "true"
ENABLE_TWITCH    = os.getenv("ENABLE_TWITCH", "true").lower() == "true"
# -----------------------------------------------------------------------------


class ChatInterface:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()

    # -- Producers -------------------------------------------------------------

    async def run_youtube(self):
        """
        Monitor YouTube chat indefinitely.
        The monitor instance is kept alive across reconnects so _last_ts
        persists and old messages are never replayed.
        """
        if not YOUTUBE_VIDEO_ID:
            print("[Chat] YOUTUBE_VIDEO_ID not set in config.env -- skipping YouTube.")
            return

        monitor = YouTubeChatMonitor(video_id=YOUTUBE_VIDEO_ID)
        original_handle = monitor.handle_message

        async def queuing_handle(chat_data, _m=monitor):
            before = len(_m.messages)
            await original_handle(chat_data)
            if len(_m.messages) > before:
                username, _, message = _m.messages[-1].partition(": ")
                await self.queue.put({
                    "platform": "youtube",
                    "username": username,
                    "message":  message,
                })

        monitor.handle_message = queuing_handle

        while True:
            try:
                print("[Chat] Starting YouTube monitor...")
                await monitor.listen()
                print(f"[Chat] YouTube stream ended. Restarting in {RESTART_DELAY} s...")
            except Exception as e:
                print(f"[Chat] YouTube error: {e}. Restarting in {RESTART_DELAY} s...")
            await asyncio.sleep(RESTART_DELAY)

    async def run_twitch(self):
        """
        Monitor Twitch chat indefinitely.
        The monitor instance is kept alive across reconnects so _last_ts
        persists and old messages are never replayed.
        """
        if not TWITCH_CHANNEL:
            print("[Chat] TWITCH_CHANNEL not set in config.env -- skipping Twitch.")
            return

        monitor = TwitchChatMonitor(channel_name=TWITCH_CHANNEL)
        original_handle = monitor.handle_message

        async def queuing_handle(raw_message, _m=monitor):
            before = len(_m.messages)
            await original_handle(raw_message)
            if len(_m.messages) > before:
                username, _, message = _m.messages[-1].partition(": ")
                await self.queue.put({
                    "platform": "twitch",
                    "username": username,
                    "message":  message,
                })

        monitor.handle_message = queuing_handle

        while True:
            try:
                print("[Chat] Starting Twitch monitor...")
                await monitor.listen()
                print(f"[Chat] Twitch connection closed. Restarting in {RESTART_DELAY} s...")
            except Exception as e:
                print(f"[Chat] Twitch error: {e}. Restarting in {RESTART_DELAY} s...")
            await asyncio.sleep(RESTART_DELAY)

    # -- WebSocket sender ------------------------------------------------------

    async def ws_worker(self):
        """
        Persistent send-only WebSocket connection to llm_interface.py.
        Reconnects automatically on any disconnect or error.
        """
        while True:
            try:
                print(f"[Chat] Connecting to LLM server at {WS_URI}...")
                async with websockets.connect(WS_URI) as ws:
                    print("[Chat] Connected to LLM server.")
                    await self._sender(ws)
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                OSError,
            ) as e:
                print(f"[Chat] WS disconnected ({e}). Reconnecting in {RESTART_DELAY} s...")
            except Exception as e:
                print(f"[Chat] WS error ({e}). Reconnecting in {RESTART_DELAY} s...")
            await asyncio.sleep(RESTART_DELAY)

    async def _sender(self, ws):
        """Drain the queue and send each message to the LLM server."""
        while True:
            msg = await self.queue.get()
            await ws.send(json.dumps(msg))
            self.queue.task_done()

    # -- Entry point -----------------------------------------------------------

    async def run(self):
        tasks = [self.ws_worker()]

        if ENABLE_YOUTUBE:
            tasks.append(self.run_youtube())
        else:
            print("[Chat] YouTube disabled (ENABLE_YOUTUBE=false in config.env)")

        if ENABLE_TWITCH:
            tasks.append(self.run_twitch())
        else:
            print("[Chat] Twitch disabled (ENABLE_TWITCH=false in config.env)")

        if len(tasks) == 1:
            print("[Chat] No platforms enabled -- set ENABLE_YOUTUBE or ENABLE_TWITCH to true in config.env")
            return

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    interface = ChatInterface()
    try:
        asyncio.run(interface.run())
    except KeyboardInterrupt:
        print("\n[Chat] Stopped.")