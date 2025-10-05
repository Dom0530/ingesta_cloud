FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENV LAST_PULL=2025-01-01T00:00:00
CMD ["python", "mysql_connector.py"]  
