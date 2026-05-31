import asyncio


class TwitchChatMonitor:
    def __init__(self, channel_name: str):
        self.host = "irc.chat.twitch.tv"
        self.port = 6667
        # 'justinfan' + random numbers allows anonymous read-only access
        self.nickname = "justinfan12345"
        self.channel = f"#{channel_name.lower()}"
        self.messages = []
        self.reader = None
        self.writer = None
        # Timestamp watermark (Unix ms) -- messages at or before this are skipped
        self._last_ts: int | None = None

    async def connect(self):
        """Connect to Twitch IRC anonymously and request IRCv3 tags."""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            # tags capability gives us tmi-sent-ts on every PRIVMSG
            self.writer.write("CAP REQ :twitch.tv/tags\r\n".encode("utf-8"))
            self.writer.write(f"NICK {self.nickname}\r\n".encode("utf-8"))
            self.writer.write(f"JOIN {self.channel}\r\n".encode("utf-8"))
            await self.writer.drain()
            print(f"[Twitch] Connected to channel: {self.channel}")
        except Exception as e:
            raise Exception(f"Failed to connect to Twitch: {e}")

    def _parse_tags(self, raw_line: str):
        """Parse IRCv3 tag prefix. Returns (tags_dict, remainder_of_line)."""
        tags = {}
        if raw_line.startswith("@"):
            tag_str, _, remainder = raw_line[1:].partition(" ")
            for pair in tag_str.split(";"):
                if "=" in pair:
                    k, _, v = pair.partition("=")
                    tags[k] = v
                else:
                    tags[pair] = ""
            return tags, remainder
        return tags, raw_line

    async def handle_message(self, raw_message):
        """
        Parse a Twitch IRC line.
        Skips silently if tmi-sent-ts is not newer than _last_ts.
        """
        tags, line = self._parse_tags(raw_message)

        if "PRIVMSG" not in line:
            return

        raw_ts = tags.get("tmi-sent-ts")
        msg_ts = None
        if raw_ts:
            try:
                msg_ts = int(raw_ts)
            except ValueError:
                pass

        # Drop if not newer than watermark
        if msg_ts is not None and self._last_ts is not None:
            if msg_ts <= self._last_ts:
                return

        if msg_ts is not None:
            self._last_ts = msg_ts

        parts = line.split(":", 2)
        if len(parts) < 3:
            return

        username = parts[1].split("!")[0]
        message  = parts[2].strip()

        if not username or not message:
            return

        self.messages.append(f"{username}: {message}")
        print(f"[Twitch] [{username}]: {message}")

    async def listen(self, duration=None):
        """Listen to chat messages."""
        try:
            await self.connect()
            start_time = asyncio.get_event_loop().time()

            while True:
                if duration and (asyncio.get_event_loop().time() - start_time) > duration:
                    print(f"[Twitch] Reached {duration}s limit. Stopping...")
                    break

                data = await self.reader.read(2048)
                if not data:
                    break

                for line in data.decode("utf-8").split("\r\n"):
                    if not line:
                        continue
                    if line.startswith("PING"):
                        self.writer.write("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
                        await self.writer.drain()
                    else:
                        await self.handle_message(line)

        except KeyboardInterrupt:
            print("[Twitch] Stopping...")
        except Exception as e:
            print(f"[Twitch] Error: {e}")
        finally:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()

    def get_messages(self):
        return self.messages


async def main():
    import os
    from dotenv import load_dotenv
    load_dotenv("config.env")
    channel = os.getenv("TWITCH_CHANNEL", "")
    if not channel:
        print("Set TWITCH_CHANNEL in config.env")
        return
    monitor = TwitchChatMonitor(channel_name=channel)
    await monitor.listen(duration=60)
    print(f"\nTotal messages: {len(monitor.get_messages())}")
    for msg in monitor.get_messages():
        print(msg)


if __name__ == "__main__":
    asyncio.run(main())