FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary Faker

COPY generator/batch_generator.py /app/batch_generator.py

ENTRYPOINT ["python", "/app/batch_generator.py"]
