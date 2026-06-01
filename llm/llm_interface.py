import asyncio
import json
import os
from pathlib import Path
import websockets
from dotenv import load_dotenv
from llm import LLM

for env_path in ["config.env", "../config.env"]:
    if Path(env_path).exists():
        load_dotenv(env_path)
        break

WS_HUB_HOST   = os.getenv("WS_HUB_HOST", "localhost")
WS_HUB_PORT   = int(os.getenv("WS_HUB_PORT", "8765"))
RESTART_DELAY = int(os.getenv("RESTART_DELAY", "5"))
WS_URI        = f"ws://{WS_HUB_HOST}:{WS_HUB_PORT}/ws/llm"


class LLMWorker:
    def __init__(self):
        self.llm = LLM()
        print("[LLM Worker] Model loaded.")

    async def run(self):
        while True:
            try:
                print(f"[LLM Worker] Connecting to hub at {WS_URI}...")
                async with websockets.connect(WS_URI) as ws:
                    print("[LLM Worker] Connected.")
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            print(f"[LLM Worker] Bad JSON: {raw!r}")
                            continue

                        platform = data.get("platform", "unknown")
                        username = data.get("username", "unknown")
                        message  = data.get("message", "")
                        msg_id   = data.get("id", "")

                        print(f"\n[LLM Worker] [{platform}] {username}: {message}")

                        prompt = f"[{platform}] {username} says: {message}"
                        try:
                            response = await self.llm.responce(prompt)
                        except Exception as e:
                            response = f"[error: {e}]"
                            print(f"[LLM Worker] LLM error: {e}")

                        print(f"[LLM Worker] Response: {response}")

                        await ws.send(json.dumps({
                            "ref_id":       msg_id,
                            "platform":     platform,
                            "username":     username,
                            "orig_message": message,
                            "response":     response,
                        }))

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                OSError,
            ) as e:
                print(f"[LLM Worker] Disconnected ({e}). Reconnecting in {RESTART_DELAY}s...")
            except Exception as e:
                print(f"[LLM Worker] Error ({e}). Reconnecting in {RESTART_DELAY}s...")
            await asyncio.sleep(RESTART_DELAY)


if __name__ == "__main__":
    worker = LLMWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        print("\n[LLM Worker] Stopped.")