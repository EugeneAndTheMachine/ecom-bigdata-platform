ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm

ARG MLFLOW_VERSION=2.22.0
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv

COPY requirements.txt .
# mlflow-skinny = registry client + pyfunc loader without the server dependencies
RUN pip install -r requirements.txt "mlflow-skinny==${MLFLOW_VERSION}"

COPY app ./app
RUN useradd --create-home --uid 10001 api
USER api
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
