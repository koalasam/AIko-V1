import asyncio
import os
import re
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv("config.env")

LLM_MODEL   = os.getenv("LLM_MODEL", "google/gemma-4-e4b")
API_KEY     = os.getenv("LLM_API_KEY", "lm-studio")   # many local servers accept any non-empty string
API_BASE    = os.getenv("LLM_API_BASE", "http://localhost:1234/v1")
SYSTEM_PROMPT = os.getenv("LLM_SYSTEM_PROMPT", "This is a test prompt to be ignored")


class LLM:
    def __init__(self):
        self.client = OpenAI(api_key=API_KEY, base_url=API_BASE)
        # Keep the same chat-history structure as before: a list of dicts
        self.history: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

    def remove_think(self, text: str) -> str:
        """Delete everything between <think> and </think> (including the tags)."""
        return re.sub(r"<think>.*?</think>", "", str(text), flags=re.DOTALL)

    async def responce(self, prompt: str) -> str:
        self.history.append({"role": "user", "content": prompt})

        # Run the blocking SDK call in a thread so the event loop stays free
        response = await asyncio.to_thread(
            lambda: self.client.chat.completions.create(
                model=LLM_MODEL,
                messages=self.history,
            )
        )

        assistant_text = response.choices[0].message.content or ""
        assistant_text = self.remove_think(assistant_text).strip()

        # Append assistant reply to history (mirrors the old chat.append behaviour)
        self.history.append({"role": "assistant", "content": assistant_text})

        return assistant_text


# Test mode
if __name__ == "__main__":
    llm = LLM()
    while True:
        user_input = input("    (leave blank to exit) User: ")
        if user_input == "":
            exit()
        print(f"    bot: {asyncio.run(llm.responce(user_input))}")