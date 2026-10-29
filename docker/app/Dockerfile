# Multi-architecture (AMD64 & ARM64 / Apple Silicon) Python runner for training, producer, and verification
FROM python:3.11-slim

WORKDIR /opt/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/app/requirements.txt

COPY . /opt/app
ENV PYTHONPATH=/opt/app
