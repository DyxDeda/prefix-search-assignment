FROM python:3.10-slim

WORKDIR /app

# Установка curl для healthcheck Elasticsearch
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем ВСЕ необходимые файлы
COPY load_and_search.py .
COPY tools/ ./tools/
COPY data/ ./data/

# Команда по умолчанию
CMD ["python", "load_and_search.py"]