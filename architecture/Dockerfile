FROM apache/airflow:3.2.0

USER root

ARG SPARK_VERSION=3.5.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
    && curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        | tar -xz -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}" \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
    && chmod -R a+rX "${SPARK_HOME}" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalación de todas las dependencias fijas para optimizar el tiempo de arranque
RUN pip install --no-cache-dir \
    'apache-airflow-core[gunicorn]' \
    gunicorn \
    pandas \
    openpyxl \
    python-dotenv \
    kaggle \
    apache-airflow-providers-amazon \
    apache-airflow-providers-celery \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-fab

# Argumento para instalar dependencias adicionales opcionales desde el .env
ARG EXTRA_REQUIREMENTS=""
RUN if [ -n "$EXTRA_REQUIREMENTS" ]; then \
        pip install --no-cache-dir $EXTRA_REQUIREMENTS ; \
    fi

WORKDIR /opt/airflow
