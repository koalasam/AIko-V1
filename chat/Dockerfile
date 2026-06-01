#Dockerfile for running chat as a container
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY chat_interface.py youtube_chat_conector.py twitch_chat_conector.py config.env ./

EXPOSE 8080

CMD ["python", "chat_interface.py"]