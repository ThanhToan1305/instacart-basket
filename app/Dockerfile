FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --only-binary=:all: -r /app/requirements.txt
COPY . /app
RUN useradd --create-home --uid 10001 appuser
USER appuser
EXPOSE 8501
CMD ["streamlit", "run", "main.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
