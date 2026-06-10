# Bronze ingestion Spark image
#
# Base: apache/spark:3.5.0-python3
# Adds: Iceberg runtime JAR, Kafka connector JARs, AWS S3A JARs, Python deps
#
# Build:
#   docker build -t localhost:5001/spark-bronze:latest \
#     -f pipelines/spark/docker/Dockerfile pipelines/spark/
#   docker push localhost:5001/spark-bronze:latest
#
# Why bake JARs in instead of spark.jars.packages?
#   spark.jars.packages downloads from Maven on every job start — 30–60s
#   of network IO before any Spark work begins. Baking JARs into the image
#   makes the download a one-time Docker build step and keeps job startup fast.
#
# Why RUN curl instead of ADD?
#   Docker's ADD instruction downloads files owned by root with mode 0600.
#   The spark user (non-root) cannot read them from the JVM classpath.
#   RUN curl lets us set permissions (0644) in the same layer.

FROM apache/spark:3.5.0-python3

USER root

ENV SPARK_HOME=/opt/spark
ENV JARS_DIR=${SPARK_HOME}/jars

# ─── JARs ─────────────────────────────────────────────────────────────────────
# All downloaded with 0644 in a single RUN so no separate chmod layer is needed.
RUN curl -fsSL -o ${JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    curl -fsSL -o ${JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o ${JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o ${JARS_DIR}/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o ${JARS_DIR}/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o ${JARS_DIR}/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o ${JARS_DIR}/aws-java-sdk-bundle-1.12.592.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.592/aws-java-sdk-bundle-1.12.592.jar && \
    chmod 644 ${JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
              ${JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
              ${JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
              ${JARS_DIR}/kafka-clients-3.4.1.jar \
              ${JARS_DIR}/commons-pool2-2.11.1.jar \
              ${JARS_DIR}/hadoop-aws-3.3.4.jar \
              ${JARS_DIR}/aws-java-sdk-bundle-1.12.592.jar

# ─── Python dependencies ───────────────────────────────────────────────────────
COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ─── Application code ─────────────────────────────────────────────────────────
COPY jobs/ /app/
WORKDIR /app

USER spark
