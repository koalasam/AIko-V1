import asyncio
import pytchat
from datetime import datetime, timezone


class YouTubeChatMonitor:
    def __init__(self, video_id: str):
        self.video_id = video_id
        self.messages = []
        self.chat = None
        # Timestamp watermark -- messages at or before this are skipped on reconnect
        self._last_ts: datetime | None = None

    async def connect(self):
        """Connect to YouTube chat."""
        try:
            self.chat = pytchat.create(video_id=self.video_id)
            print(f"[YouTube] Connected to video: {self.video_id}")
        except Exception as e:
            raise Exception(f"Failed to connect to YouTube chat: {e}")

    async def handle_message(self, chat_data):
        """
        Parse and handle an incoming chat message.
        Skips silently if the message timestamp is not newer than _last_ts.
        """
        try:
            username = chat_data.author.name
            message  = chat_data.message

            if not username or not message:
                return

            raw_ts = getattr(chat_data, "datetime", None)
            msg_ts = None
            if raw_ts:
                try:
                    msg_ts = datetime.fromisoformat(str(raw_ts))
                    if msg_ts.tzinfo is None:
                        msg_ts = msg_ts.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            # Drop if not newer than watermark
            if msg_ts is not None and self._last_ts is not None:
                if msg_ts <= self._last_ts:
                    return

            if msg_ts is not None:
                self._last_ts = msg_ts

            self.messages.append(f"{username}: {message}")
            print(f"[YouTube] [{username}]: {message}")

        except Exception as e:
            print(f"[YouTube] Error parsing message: {e}")

    async def listen(self, duration=None):
        """Listen to chat messages."""
        try:
            await self.connect()
            start_time = asyncio.get_event_loop().time()

            while self.chat.is_alive():
                if duration and (asyncio.get_event_loop().time() - start_time) > duration:
                    print(f"[YouTube] Reached {duration}s limit. Stopping...")
                    break
                try:
                    for chat_data in self.chat.get().sync_items():
                        await self.handle_message(chat_data)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"[YouTube] Error receiving message: {e}")
                    await asyncio.sleep(1)

        except KeyboardInterrupt:
            print("[YouTube] Stopping...")
        except Exception as e:
            print(f"[YouTube] Error: {e}")
        finally:
            if self.chat:
                self.chat.terminate()

    def get_messages(self):
        return self.messages


async def main():
    import os
    from dotenv import load_dotenv
    load_dotenv("config.env")
    video_id = os.getenv("YOUTUBE_VIDEO_ID", "")
    if not video_id:
        print("Set YOUTUBE_VIDEO_ID in config.env")
        return
    monitor = YouTubeChatMonitor(video_id=video_id)
    await monitor.listen()
    print(f"\nTotal messages: {len(monitor.get_messages())}")
    for msg in monitor.get_messages():
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())