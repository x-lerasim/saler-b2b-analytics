FROM python:3.10-slim

ARG DBT_VERSION=1.9.8

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-clickhouse==${DBT_VERSION}


RUN useradd -ms /bin/bash dbt
USER dbt

WORKDIR /usr/app
