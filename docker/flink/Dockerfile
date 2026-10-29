# Multi-architecture (AMD64 & ARM64 / Apple Silicon) Apache Flink 1.18.1 + PyFlink + Kafka + Iceberg
FROM flink:1.18.1-scala_2.12-java11

USER root

# Install Python 3 and build prerequisites for PyFlink 1.18.1
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    ca-certificates \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir --upgrade pip setuptools wheel \
    && pip3 install --no-cache-dir \
        apache-flink==1.18.1 \
        python-dotenv==1.0.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download mutually compatible, pinned connector JARs into /opt/flink/lib:
# - flink-sql-connector-kafka-3.1.0-1.18.jar (Flink 1.18 Kafka connector)
# - iceberg-flink-runtime-1.18-1.5.2.jar (Apache Iceberg 1.5.2 runtime for Flink 1.18)
# - iceberg-aws-bundle-1.5.2.jar (S3FileIO bundle for MinIO/S3 integration)
# - hadoop-client-api-3.3.6.jar & hadoop-client-runtime-3.3.6.jar (required by Iceberg Flink runtime)
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
        -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar \
        -o /opt/flink/lib/iceberg-flink-runtime-1.18-1.5.2.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
        -o /opt/flink/lib/iceberg-aws-bundle-1.5.2.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
        -o /opt/flink/lib/hadoop-client-api-3.3.6.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
        -o /opt/flink/lib/hadoop-client-runtime-3.3.6.jar && \
    chown -R flink:flink /opt/flink/lib

WORKDIR /opt/app
USER flink
