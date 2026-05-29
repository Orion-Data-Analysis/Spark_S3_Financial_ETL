FROM apache/airflow:3.2.0

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
    apache-airflow-providers-apache-spark

# Argumento para instalar dependencias adicionales opcionales desde el .env
ARG EXTRA_REQUIREMENTS=""
RUN if [ -n "$EXTRA_REQUIREMENTS" ]; then \
        pip install --no-cache-dir $EXTRA_REQUIREMENTS ; \
    fi

WORKDIR /opt/airflow
