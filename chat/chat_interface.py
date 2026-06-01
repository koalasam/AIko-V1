import asyncio
import json
import os
from pathlib import Path
import websockets
from dotenv import load_dotenv
from youtube_chat_conector import YouTubeChatMonitor
from twitch_chat_conector import TwitchChatMonitor

for env_path in ["config.env", "../config.env"]:
    if Path(env_path).exists():
        load_dotenv(env_path)
        break

YOUTUBE_VIDEO_ID = os.getenv("YOUTUBE_VIDEO_ID", "")
TWITCH_CHANNEL   = os.getenv("TWITCH_CHANNEL", "")
WS_HUB_HOST      = os.getenv("WS_HUB_HOST", "localhost")
WS_HUB_PORT      = int(os.getenv("WS_HUB_PORT", "8765"))
RESTART_DELAY    = int(os.getenv("RESTART_DELAY", "5"))
ENABLE_YOUTUBE   = os.getenv("ENABLE_YOUTUBE", "true").lower() == "true"
ENABLE_TWITCH    = os.getenv("ENABLE_TWITCH",  "true").lower() == "true"

WS_URI = f"ws://{WS_HUB_HOST}:{WS_HUB_PORT}/ws/chat"


class ChatInterface:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()

    async def run_youtube(self):
        if not YOUTUBE_VIDEO_ID:
            print("[Chat] YOUTUBE_VIDEO_ID not set -- skipping YouTube.")
            return
        monitor = YouTubeChatMonitor(video_id=YOUTUBE_VIDEO_ID)
        original_handle = monitor.handle_message

        async def queuing_handle(chat_data, _m=monitor):
            before = len(_m.messages)
            await original_handle(chat_data)
            if len(_m.messages) > before:
                username, _, message = _m.messages[-1].partition(": ")
                await self.queue.put({"platform": "youtube", "username": username, "message": message})

        monitor.handle_message = queuing_handle
        while True:
            try:
                print("[Chat] Starting YouTube monitor...")
                await monitor.listen()
                print(f"[Chat] YouTube stream ended. Restarting in {RESTART_DELAY}s...")
            except Exception as e:
                print(f"[Chat] YouTube error: {e}. Restarting in {RESTART_DELAY}s...")
            await asyncio.sleep(RESTART_DELAY)

    async def run_twitch(self):
        if not TWITCH_CHANNEL:
            print("[Chat] TWITCH_CHANNEL not set -- skipping Twitch.")
            return
        monitor = TwitchChatMonitor(channel_name=TWITCH_CHANNEL)
        original_handle = monitor.handle_message

        async def queuing_handle(raw_message, _m=monitor):
            before = len(_m.messages)
            await original_handle(raw_message)
            if len(_m.messages) > before:
                username, _, message = _m.messages[-1].partition(": ")
                await self.queue.put({"platform": "twitch", "username": username, "message": message})

        monitor.handle_message = queuing_handle
        while True:
            try:
                print("[Chat] Starting Twitch monitor...")
                await monitor.listen()
                print(f"[Chat] Twitch closed. Restarting in {RESTART_DELAY}s...")
            except Exception as e:
                print(f"[Chat] Twitch error: {e}. Restarting in {RESTART_DELAY}s...")
            await asyncio.sleep(RESTART_DELAY)

    async def ws_worker(self):
        while True:
            try:
                print(f"[Chat] Connecting to hub at {WS_URI}...")
                async with websockets.connect(WS_URI) as ws:
                    print("[Chat] Connected to hub.")
                    while True:
                        msg = await self.queue.get()
                        await ws.send(json.dumps(msg))
                        self.queue.task_done()
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                OSError,
            ) as e:
                print(f"[Chat] WS disconnected ({e}). Reconnecting in {RESTART_DELAY}s...")
            except Exception as e:
                print(f"[Chat] WS error ({e}). Reconnecting in {RESTART_DELAY}s...")
            await asyncio.sleep(RESTART_DELAY)

    async def run(self):
        tasks = [self.ws_worker()]
        if ENABLE_YOUTUBE:
            tasks.append(self.run_youtube())
        else:
            print("[Chat] YouTube disabled.")
        if ENABLE_TWITCH:
            tasks.append(self.run_twitch())
        else:
            print("[Chat] Twitch disabled.")
        if len(tasks) == 1:
            print("[Chat] No platforms enabled.")
            return
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    interface = ChatInterface()
    try:
        asyncio.run(interface.run())
    except KeyboardInterrupt:
        print("\n[Chat] Stopped.")