"""
llm_interface.py -- WebSocket server
Receives chat messages from chat_interface.py, generates LLM responses,
and prints each received message followed immediately by the response.

All configuration is loaded from config.env (same directory).

Runs indefinitely until stopped with Ctrl+C.

Usage:
    python llm_interface.py
"""

import asyncio
import json
import os
import websockets
from dotenv import load_dotenv
from llm import LLM

# -- Load config --------------------------------------------------------------
load_dotenv("config.env")

WS_HOST = os.getenv("WS_HOST", "localhost")
WS_PORT = int(os.getenv("WS_PORT", "8765"))
# -----------------------------------------------------------------------------


class LLMServer:
    def __init__(self):
        self.llm = LLM()
        print("[LLM Server] Model loaded.")

    async def handle_client(self, websocket):
        """Handle an incoming WebSocket connection from chat_interface."""
        client_addr = websocket.remote_address
        print(f"[LLM Server] Client connected: {client_addr}")

        try:
            async for raw in websocket:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[LLM Server] Bad JSON: {raw!r}")
                    continue

                platform = data.get("platform", "unknown")
                username = data.get("username", "unknown")
                message  = data.get("message", "")

                print(f"\nReceived [{platform}] {username}: {message}")

                prompt = f"[{platform}] {username} says: {message}"
                try:
                    response = await self.llm.responce(prompt)
                except Exception as e:
                    response = f"[error: {e}]"
                    print(f"[LLM Server] LLM error: {e}")

                print(f"Response: {response}\n")

        except websockets.exceptions.ConnectionClosedOK:
            print(f"[LLM Server] Client disconnected: {client_addr}")
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[LLM Server] Connection error ({client_addr}): {e}")

    async def run(self):
        print(f"[LLM Server] Listening on ws://{WS_HOST}:{WS_PORT}")
        async with websockets.serve(self.handle_client, WS_HOST, WS_PORT):
            await asyncio.Future()


if __name__ == "__main__":
    server = LLMServer()
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\n[LLM Server] Stopped.")