# llm_openai_compatible/Dockerfile
# Runs llm_interface.py (WebSocket server) which calls any OpenAI-compatible
# LLM endpoint via llm.py (LM Studio, Ollama, OpenAI, vLLM, etc.)

FROM python:3.11-slim

# netcat is used by the docker-compose healthcheck (nc -z localhost 8765)
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies (layer-cached until requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY llm.py llm_interface.py config.env ./

# Defaults -- all overridden by docker-compose environment block at runtime
ENV WS_HOST=0.0.0.0
ENV WS_PORT=8765
ENV LLM_API_BASE=http://host.docker.internal:1234/v1
ENV LLM_API_KEY=lm-studio
ENV LLM_MODEL=google/gemma-4-e4b
ENV LLM_SYSTEM_PROMPT="You are a helpful assistant."

EXPOSE 8765

CMD ["python", "llm_interface.py"]