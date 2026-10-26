ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY services/clickstream-producer/requirements.txt .
RUN pip install -r requirements.txt

COPY services/clickstream-producer/producer ./producer
COPY schemas /opt/app/schemas

RUN useradd --create-home --uid 10001 app
USER app
EXPOSE 8001
ENTRYPOINT ["python", "-m", "producer"]
