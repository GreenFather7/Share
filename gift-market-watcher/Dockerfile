FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gmw ./gmw
# Сессии Telegram живут в смонтированной папке, не в образе.
WORKDIR /app/sessions
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "gmw"]
