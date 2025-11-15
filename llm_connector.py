from system_prompt import SYSTEM_PROMPT
from config import LLM_MODEL
import lmstudio as lms
import asyncio
import re


class LLM():
    def __init__ (self):
        self.model = lms.llm(LLM_MODEL)
        self.chat = lms.Chat(SYSTEM_PROMPT)

    def remove_think(self, text: str):
        """Delete everything between <think> and </think> (including the tags)."""
        return re.sub(r"<think>.*?</think>", "", str(text), flags=re.DOTALL)
    
    async def responce (self, prompt: str):
            self.chat.add_user_message(prompt)
            return self.remove_think(self.model.respond(self.chat,on_message=self.chat.append,)).strip()

# Test_Mode
if __name__ == "__main__":
    llm = LLM()
    while True:
        user_input = input("(leave blank to exit) User: ")
        if user_input == "":
            exit()
        print(f"bot: {asyncio.run(llm.responce(user_input))}")