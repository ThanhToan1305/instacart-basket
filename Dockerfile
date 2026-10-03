FROM python:3.14.4-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SPARK_LOCAL_IP=127.0.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk-headless \
       procps \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYSPARK_PYTHON="/app/.venv/bin/python"

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && python -m pip check

COPY . .

RUN sed -i 's/\r$//' run_pipeline.sh

EXPOSE 8501

ENTRYPOINT ["python", "/app/docker_entrypoint.py"]
CMD ["dashboard"]