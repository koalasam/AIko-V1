import os
import asyncio
from pathlib import Path

import aiofiles
import aiohttp
from dotenv import load_dotenv


class TTS:
    def __init__(self, env_file: str = "config.env"):
        load_dotenv(env_file)

        self.base_url = os.getenv("GPT_SOVITS_URL", "http://127.0.0.1:9880")

        self.reference_audio = os.getenv("REFERENCE_AUDIO")
        self.reference_text = os.getenv("REFERENCE_TEXT")
        self.reference_language = os.getenv("REFERENCE_LANGUAGE", "en")

        self.output_dir = Path(
            os.getenv("OUTPUT_DIR", "output")
        )

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        if not self.reference_audio:
            raise ValueError(
                "REFERENCE_AUDIO missing from config.env"
            )

        if not self.reference_text:
            raise ValueError(
                "REFERENCE_TEXT missing from config.env"
            )

    async def speak(
        self,
        text: str,
        output_file: str | None = None,
        text_language: str = "en"
    ) -> str:

        if output_file is None:
            output_file = (
                self.output_dir /
                f"tts_{int(asyncio.get_running_loop().time() * 1000)}.wav"
            )
        else:
            output_file = Path(output_file)

        payload = {
            "text": text,
            "text_lang": text_language,
            "ref_audio_path": self.reference_audio,
            "prompt_text": self.reference_text,
            "prompt_lang": self.reference_language,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/tts",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=300)
            ) as response:

                if response.status != 200:
                    error = await response.text()
                    raise RuntimeError(
                        f"GPT-SoVITS API Error ({response.status}): {error}"
                    )

                audio_data = await response.read()

        async with aiofiles.open(
            output_file,
            "wb"
        ) as f:
            await f.write(audio_data)

        return str(output_file)

    async def speak_and_play(
        self,
        text: str,
        text_language: str = "en"
    ) -> str:

        output = await self.speak(
            text=text,
            text_language=text_language
        )

        if os.name == "posix":
            proc = await asyncio.create_subprocess_exec(
                "ffplay",
                "-nodisp",
                "-autoexit",
                output,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()

        return output


async def main():
    tts = TTS()

    result = await tts.speak_and_play(
        "Hello, this is a test of GPT SoVITS."
    )

    print(
        f"Generated audio: {result}"
    )


if __name__ == "__main__":
    asyncio.run(main())