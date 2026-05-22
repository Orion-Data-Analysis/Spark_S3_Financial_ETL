Sí. Para hacerlo 100% automático con Airflow desde Kaggle hasta landing, el flujo sería:

Airflow
  -> descarga datasets desde Kaggle
  -> descomprime archivos
  -> sube archivos originales a S3 landing
  -> genera manifest de ingesta
  -> dispara pipeline landing -> raw -> staging -> mart -> consumption
1. Definir el flujo objetivo

Tu DAG debería quedar así:

download_kaggle_sources
  -> unzip_kaggle_sources
  -> upload_sources_to_landing
  -> validate_landing_files
  -> landing_to_raw
  -> raw_to_staging
  -> staging_to_intermediate
  -> intermediate_to_mart
  -> mart_to_consumption
La parte nueva sería hasta landing. El resto usa tu pipeline actual.

2. Guardar credenciales de Kaggle de forma segura

En Kaggle generas el token:

Kaggle -> Settings -> API -> Create New Token
Obtendrás:

{
  "username": "tu_usuario",
  "key": "tu_api_key"
}
En Airflow no lo dejes en código. Guárdalo como Variables o Connections:

KAGGLE_USERNAME
KAGGLE_KEY
Por ejemplo en Airflow UI:

Admin -> Variables
Crea:

kaggle_username=tu_usuario
kaggle_key=tu_key
3. Configurar acceso AWS en Airflow

Lo ideal es una conexión:

aws_default
En Airflow UI:

Admin -> Connections
Configura:

Conn Id: aws_default
Conn Type: Amazon Web Services
Region: us-east-2
Si estás en AWS, mejor usar IAM Role. Si estás local, puedes usar access key y secret key.

4. Instalar dependencias en el contenedor de Airflow

Tu imagen de Airflow debe tener:

kaggle
awscli
apache-airflow-providers-amazon
En el Dockerfile podrías agregar algo así:

RUN pip install --no-cache-dir kaggle awscli apache-airflow-providers-amazon
Luego reconstruyes:

docker compose build
docker compose up -d
5. Definir variables del proyecto

En Airflow Variables:

financial_crisis_bucket=orion-financial-crisis-data-395840094505-us-east-2-an
financial_crisis_env=dev
financial_crisis_domain=financial_crisis
Opcional:

financial_crisis_landing_prefix=dev/financial_crisis/landing
6. Crear carpeta temporal dentro de Airflow

El DAG puede trabajar temporalmente en:

/tmp/kaggle_financial_crisis
Ahí descarga y descomprime antes de subir a S3.

7. Crear DAG de extracción Kaggle a landing

La lógica sería:

from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import os
import subprocess
import zipfile
from pathlib import Path

BUCKET = "orion-financial-crisis-data-395840094505-us-east-2-an"

@dag(
    dag_id="kaggle_to_s3_landing_financial_crisis",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["financial_crisis", "kaggle", "landing"],
)
def kaggle_to_s3_landing():

    @task
    def configure_kaggle():
        kaggle_dir = Path.home() / ".kaggle"
        kaggle_dir.mkdir(parents=True, exist_ok=True)

        username = Variable.get("kaggle_username")
        key = Variable.get("kaggle_key")

        kaggle_json = kaggle_dir / "kaggle.json"
        kaggle_json.write_text(
            f'{{"username":"{username}","key":"{key}"}}',
            encoding="utf-8"
        )
        os.chmod(kaggle_json, 0o600)

    @task
    def download_sources():
        base = Path("/tmp/kaggle_financial_crisis")
        sources = {
            "ieee_cis_fraud_detection": base / "ieee_cis",
            "credit_card_fraud_detection": base / "credit_card",
            "paysim": base / "paysim",
        }

        for path in sources.values():
            path.mkdir(parents=True, exist_ok=True)

        subprocess.run([
            "kaggle", "competitions", "download",
            "-c", "ieee-fraud-detection",
            "-p", str(sources["ieee_cis_fraud_detection"])
        ], check=True)

        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "mlg-ulb/creditcardfraud",
            "-p", str(sources["credit_card_fraud_detection"])
        ], check=True)

        subprocess.run([
            "kaggle", "datasets", "download",
            "-d", "ealaxi/paysim1",
            "-p", str(sources["paysim"])
        ], check=True)

        return {name: str(path) for name, path in sources.items()}

    @task
    def unzip_sources(source_paths: dict):
        for path_text in source_paths.values():
            path = Path(path_text)
            for zip_file in path.glob("*.zip"):
                with zipfile.ZipFile(zip_file, "r") as archive:
                    archive.extractall(path)

        return source_paths

    @task
    def upload_to_landing(source_paths: dict):
        bucket = Variable.get("financial_crisis_bucket")
        env = Variable.get("financial_crisis_env", default_var="dev")
        domain = Variable.get("financial_crisis_domain", default_var="financial_crisis")

        ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
        run_id = datetime.utcnow().strftime("airflow_%Y%m%dT%H%M%S")

        for source_system, local_path in source_paths.items():
            s3_uri = (
                f"s3://{bucket}/{env}/{domain}/landing/"
                f"source_system={source_system}/"
                f"ingestion_date={ingestion_date}/"
                f"run_id={run_id}/"
            )

            subprocess.run([
                "aws", "s3", "sync",
                local_path,
                s3_uri,
                "--exclude", "*.zip"
            ], check=True)

    configure = configure_kaggle()
    downloaded = download_sources()
    unzipped = unzip_sources(downloaded)
    configure >> downloaded >> unzipped >> upload_to_landing(unzipped)

kaggle_to_s3_landing()
8. Validar archivos en landing

Después de subir, agrega una tarea que confirme que existan:

train_transaction.csv
train_identity.csv
test_transaction.csv
test_identity.csv
creditcard.csv
PS_20174392719_1491204439457_log.csv
Si falta alguno, el DAG debe fallar.

9. Crear manifest de landing

Muy recomendable guardar un archivo como:

landing/.../run_id=airflow_20260521T120000/_manifest.json
Con datos como:

{
  "source_system": "paysim",
  "ingestion_date": "2026-05-21",
  "run_id": "airflow_20260521T120000",
  "source": "kaggle",
  "files": ["PS_20174392719_1491204439457_log.csv"]
}
10. Conectar landing con el resto del pipeline

Tu DAG principal debería arrancar desde S3 landing:

validate_landing_files
  -> landing_to_raw
  -> clean_and_validate_sources
  -> integrate_and_model_financial_events
  -> generate_business_views
  -> publish_information_consumption
La tarea landing_to_raw copia desde:

dev/financial_crisis/landing/source_system=.../
hacia:

dev/financial_crisis/raw/source_system=.../
Y desde raw ya puedes transformar normalmente.

Recomendación práctica

Para tu proyecto, haría dos DAGs:

1. kaggle_to_s3_landing_financial_crisis
2. digital_financial_crisis_full_flow
El primero carga los datos externos.
El segundo procesa las capas del Data Lake.

Así queda claro y defendible: Kaggle es extracción, landing es recepción original, y tu pipeline de datos empieza desde landing.





