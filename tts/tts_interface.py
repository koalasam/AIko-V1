"""
tts_interface.py — TTS Worker
Connects to the WebSocket hub as role "tts".
Receives LLM responses, generates audio via GPT-SoVITS,
then forwards the audio file path to the audio streamer hub on port 8081.
"""

import asyncio
import json
import os
from pathlib import Path

import websockets
from dotenv import load_dotenv
from tts import TTS

for env_path in ["config.env", "../config.env"]:
    if Path(env_path).exists():
        load_dotenv(env_path)
        break

WS_HUB_HOST      = os.getenv("WS_HUB_HOST", "localhost")
WS_HUB_PORT      = int(os.getenv("WS_HUB_PORT", "8765"))
AUDIO_HUB_HOST   = os.getenv("AUDIO_HUB_HOST", "localhost")
AUDIO_HUB_PORT   = int(os.getenv("AUDIO_HUB_PORT", "8081"))
RESTART_DELAY    = int(os.getenv("RESTART_DELAY", "5"))
TTS_ENABLED      = os.getenv("TTS_ENABLED", "true").lower() == "true"

WS_URI       = f"ws://{WS_HUB_HOST}:{WS_HUB_PORT}/ws/tts"
AUDIO_WS_URI = f"ws://{AUDIO_HUB_HOST}:{AUDIO_HUB_PORT}/ws/audio"


class TTSWorker:
    def __init__(self):
        if not TTS_ENABLED:
            print("[TTS Worker] TTS disabled via config.")
            return
        self.tts = TTS()
        print("[TTS Worker] TTS loaded.")

    async def send_to_audio_streamer(self, audio_path: str, meta: dict):
        """Forward generated audio file path to the audio streamer."""
        payload = {
            "audio_path": audio_path,
            "ref_id":     meta.get("ref_id", ""),
            "platform":   meta.get("platform", ""),
            "username":   meta.get("username", ""),
            "response":   meta.get("response", ""),
        }
        try:
            async with websockets.connect(AUDIO_WS_URI) as ws:
                await ws.send(json.dumps(payload))
                print(f"[TTS Worker] Sent audio to streamer: {audio_path}")
        except Exception as e:
            print(f"[TTS Worker] Could not reach audio streamer: {e}")

    async def run(self):
        if not TTS_ENABLED:
            print("[TTS Worker] TTS disabled, exiting.")
            return

        while True:
            try:
                print(f"[TTS Worker] Connecting to hub at {WS_URI}...")
                async with websockets.connect(WS_URI) as ws:
                    print("[TTS Worker] Connected.")
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            print(f"[TTS Worker] Bad JSON: {raw!r}")
                            continue

                        response_text = data.get("response", "")
                        if not response_text:
                            continue

                        platform = data.get("platform", "unknown")
                        username = data.get("username", "unknown")
                        print(f"[TTS Worker] Synthesising for [{platform}] {username}: {response_text[:60]}...")

                        try:
                            audio_path = await self.tts.speak(text=response_text)
                            print(f"[TTS Worker] Audio saved: {audio_path}")
                            await self.send_to_audio_streamer(audio_path, data)
                        except Exception as e:
                            print(f"[TTS Worker] TTS error: {e}")

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                OSError,
            ) as e:
                print(f"[TTS Worker] Disconnected ({e}). Reconnecting in {RESTART_DELAY}s...")
            except Exception as e:
                print(f"[TTS Worker] Error ({e}). Reconnecting in {RESTART_DELAY}s...")
            await asyncio.sleep(RESTART_DELAY)


if __name__ == "__main__":
    worker = TTSWorker()
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        print("\n[TTS Worker] Stopped.")