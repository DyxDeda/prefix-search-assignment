FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_and_search.py .
COPY tools/ ./tools/
COPY data/ ./data/

CMD ["python", "load_and_search.py"]