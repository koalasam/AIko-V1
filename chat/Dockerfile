# chat/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY chat_interface.py youtube_chat_conector.py twitch_chat_conector.py config.env ./

ENV WS_HOST=llm_lmstudio
ENV WS_PORT=8765

CMD ["python", "chat_interface.py"]