FROM python:3.11-slim-bookworm

ARG SPARK_UID=1000
ARG SPARK_GID=1000

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates-java curl procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && curl -fsSL https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar -o /usr/local/lib/python3.11/site-packages/pyspark/jars/delta-storage-3.2.1.jar
RUN groupadd --non-unique --gid "${SPARK_GID}" spark \
    && useradd --create-home --uid "${SPARK_UID}" --gid spark spark

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
WORKDIR /workspace
USER spark
