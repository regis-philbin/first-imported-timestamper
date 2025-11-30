FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies
RUN pip install --no-cache-dir requests

COPY app.py /app/app.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]
