FROM python:3.12-slim

WORKDIR /app

# build deps (optional but safe)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# project files will be bind-mounted in compose, but keep for CI compatibility
COPY . /app

ENV PYTHONPATH=/app/src
CMD ["python", "-c", "import pandas; print('container ok')"]
