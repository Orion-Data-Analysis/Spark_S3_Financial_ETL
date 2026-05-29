# =========================
# CORE
# =========================
PYTHONPATH=/opt/pipelines
AIRFLOW_UID=50000

# =========================
# WORKER CONFIG
# =========================
CELERY_HOSTNAME=worker1
QUEUES=default,test1

# =========================
# AIRFLOW CORE
# =========================
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__FERNET_KEY=wNUxDHJUk1i5LUF9NIR3QS6QLkVEMjqHbYf6tf44XnY=
AIRFLOW__CORE__DAGS_FOLDER=/opt/pipelines/dags
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

# =========================
# DATABASE
# =========================
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://orion:Orion123%21@23.0.1.199:5432/orion_db
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=5
AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=10

# =========================
# CELERY
# =========================
AIRFLOW__CELERY__BROKER_URL=amqp://airflow:airflow@23.0.2.75:5672//
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql+psycopg2://orion:Orion123%21@23.0.1.199:5432/orion_db

# =========================
# API / WEB
# =========================
AIRFLOW__API__BASE_URL=https://orion-airflow.coderhivex.com/
AIRFLOW__API__INSTANCE_NAME=Airflow Data Platform
AIRFLOW__API__SECRET_KEY=6f2bff1465be0ba76111f6d3b8b60be0fc11a9f15464f0e49188324f61fc399a
AIRFLOW__API__SERVER_TYPE=gunicorn

AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt
AIRFLOW__API_AUTH__JWT_ISSUER=https://orion-airflow.coderhivex.com/execution/

AIRFLOW__CORE__EXECUTION_API_SERVER_URL=https://orion-airflow.coderhivex.com/execution/
AIRFLOW__CORE__INTERNAL_API_URL=https://orion-airflow.coderhivex.com
# =========================
# SCHEDULER
# =========================
AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=300

# =========================
# CLI / INIT
# =========================
_AIRFLOW_DB_MIGRATE=false
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
_AIRFLOW_WWW_USER_CREATE=true
_AIRFLOW_WWW_USER_USERNAME=orion.riwi
_AIRFLOW_WWW_USER_PASSWORD=Orion123!
_AIRFLOW_WWW_USER_EMAIL=orionriwi@gmail.com
_AIRFLOW_WWW_USER_FIRSTNAME=Orion
_AIRFLOW_WWW_USER_LASTNAME=Riwi
_AIRFLOW_WWW_USER_ROLE=Admin

# =========================
# LOGGING
# =========================
AIRFLOW__LOGGING__REMOTE_LOGGING=true
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/airflow
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_orion_s3
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=false

# =========================
# FINANCIAL DATA LAKE S3
# =========================
FINANCIAL_ENV=dev
FINANCIAL_AWS_REGION=us-east-2
FINANCIAL_S3_BUCKET=orion-financial-crisis-data-395840094505-us-east-2-an
FINANCIAL_S3_DOMAIN=financial_crisis
FINANCIAL_S3_BASE=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
FINANCIAL_INPUT_PATH=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing
FINANCIAL_DATA_LAKE_ROOT=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis

# Airflow AWS connection backed by the EC2 IAM Role. Do not add access keys here.
AIRFLOW_CONN_AWS_ORION_S3={"conn_type":"aws","extra":{"region_name":"us-east-2"}}

# =========================
# EXTRA (solo si necesitas)
# =========================
#  Ya estan en el dockerfile
# EXTRA_REQUIREMENTS=pandas openpyxl apache-airflow[gunicorn] apache-airflow-providers-amazon apache-airflow-providers-celery

# =========================
# SPARK STANDALONE
# =========================
# Este mismo .env lo pueden usar Airflow Master + Spark Master y Airflow Workers + Spark Workers.
SPARK_IMAGE=orion/spark-s3a:3.5.0
SPARK_NETWORK=spark-net

# Para el contenedor del Spark Master en la EC2 de Airflow Master.
SPARK_MASTER_BIND_HOST=spark-master

# Para los Spark Workers en otras EC2.
# Reemplaza este valor por la IP privada real de la EC2 donde esta Airflow Master + Spark Master.
SPARK_MASTER_CONNECT_HOST=10.0.2.251
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8082

# Si hay un solo Spark Worker por EC2, puedes dejar estos valores iguales en todas las EC2 worker.
WORKER_ID=1
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_WEBUI_PORT=8081
SPARK_WORKER_HOST=0.0.0.0

# Estas rutas son relativas al docker-compose que ejecutes.
SPARK_LOGS_PATH=../runtime/logs
SPARK_WORK_PATH=../runtime/work

SPARK_RPC_AUTH=no
SPARK_RPC_ENC=no
SPARK_STORAGE_ENC=no
SPARK_SSL=no
RESTART_POLICY=unless-stopped
