# Pipeline Kaggle -> S3 Landing -> Raw

## Objetivo

El flujo completo queda automatizado desde la descarga de Kaggle hasta la escritura de la capa Raw:

```text
1. Automatizar descarga desde Kaggle
2. Subir automaticamente a S3 Landing
3. Generar manifest de Landing
4. Ejecutar validate_landing_files
5. Ejecutar Spark hacia Raw
```

La implementacion principal esta en:

```text
architecture/pipelines/dags/financial_crisis_kaggle_to_raw.py
architecture/pipelines/spark_jobs/landing_to_raw_financial_crisis.py
architecture/pipelines/common/financial_landing.py
```

## DAG principal

El DAG que ejecuta el flujo completo es:

```text
financial_crisis_kaggle_to_raw
```

Tareas:

```text
build_landing_context
  -> configure_kaggle_credentials
  -> download_kaggle_sources
  -> unzip_kaggle_sources
  -> upload_sources_to_landing
  -> generate_landing_manifest
  -> validate_landing_files
  -> spark_landing_to_raw
```

## Fuentes descargadas

| Source system | Kaggle | Tipo | Archivo cargado a Landing |
|---|---|---|---|
| `ieee_cis_fraud_detection` | `ieee-fraud-detection` | Competition | `train_transaction.csv` |
| `credit_card_fraud_detection` | `mlg-ulb/creditcardfraud` | Dataset | `creditcard.csv` |
| `paysim` | `ealaxi/paysim1` | Dataset | `PS_20174392719_1491204439457_log.csv` |

## Rutas S3 Landing

Los archivos se suben con este contrato:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=<source_system>/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/<file_name>
```

Ejemplo:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=paysim/ingestion_date=2026-05-29/run_id=manual__2026-05-29T12:00:00+00:00/PS_20174392719_1491204439457_log.csv
```

## Manifest de Landing

Despues de subir los archivos, el DAG genera:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/_manifests/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/landing_manifest.json
```

El manifest incluye:

```json
{
  "layer": "landing",
  "domain": "financial_crisis",
  "environment": "dev",
  "bucket": "orion-financial-crisis-data-395840094505-us-east-2-an",
  "ingestion_date": "2026-05-29",
  "run_id": "manual__2026-05-29T12:00:00+00:00",
  "source": "kaggle",
  "created_at_utc": "2026-05-29T17:00:00+00:00",
  "files": []
}
```

## Validacion antes de Spark

La tarea `validate_landing_files` reutiliza la logica de:

```text
architecture/pipelines/common/financial_landing.py
```

Valida:

- existencia de los 3 archivos esperados;
- tamano mayor a cero;
- logs descriptivos por `source_system`, `file_name` y `s3_uri`;
- detencion del pipeline antes de Spark si algo falta.

## Escritura Raw con Spark

La tarea `spark_landing_to_raw` ejecuta:

```text
/opt/pipelines/spark_jobs/landing_to_raw_financial_crisis.py
```

Lee desde Landing con `s3a://`, agrega metadata tecnica y escribe Parquet en Raw.

Rutas Raw:

```text
s3a://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/raw/ieee_cis_fraud_detection/train_transaction/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/

s3a://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/raw/credit_card_fraud_detection/transactions/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/

s3a://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/raw/paysim/transactions/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/
```

Columnas tecnicas agregadas:

```text
source_system
raw_dataset
source_file_name
landing_path
ingestion_date
run_id
raw_ingestion_time
```

## Configuracion necesaria

### 1. Reconstruir la imagen de Airflow

Se agregaron dependencias al Dockerfile:

```text
kaggle
apache-airflow-providers-apache-spark
```

Reconstruye y levanta Airflow:

```bash
cd architecture/master
docker compose build
docker compose up -d
```

### 2. Configurar credenciales Kaggle

Kaggle puede entregar credenciales de dos formas.

Formato nuevo recomendado:

En Airflow UI, crea esta Variable:

```text
kaggle_api_token=<token KGAT generado por Kaggle>
```

El DAG guardara ese valor en:

```text
~/.kaggle/access_token
```

Formato legado:

Si Kaggle te entrega un archivo `kaggle.json`, crea estas dos Variables:

```text
kaggle_username=<tu_usuario_kaggle>
kaggle_key=<tu_api_key_kaggle>
```

Tambien se pueden usar variables de entorno:

```text
KAGGLE_API_TOKEN=<token KGAT generado por Kaggle>
KAGGLE_USERNAME=<tu_usuario_kaggle>
KAGGLE_KEY=<tu_api_key_kaggle>
```

Para IEEE-CIS, la cuenta de Kaggle debe haber aceptado las reglas de la competencia `ieee-fraud-detection`.

### 3. Configurar conexion Spark

En Airflow UI, crea la conexion:

```text
Connection Id: spark_standalone
Connection Type: Spark
Host: spark://<ip-privada-spark-master>
Port: 7077
```

Si el provider pide host sin protocolo:

```text
Host: <ip-privada-spark-master>
Port: 7077
Extra: {"deploy-mode": "client"}
```

### 4. Configurar AWS

La conexion AWS usada es:

```text
aws_orion_s3
```

Debe tener permisos para:

```text
s3:PutObject
s3:GetObject
s3:ListBucket
s3:HeadObject
```

Sobre el bucket:

```text
orion-financial-crisis-data-395840094505-us-east-2-an
```

## Ejecucion

Desde Airflow UI, ejecuta el DAG:

```text
financial_crisis_kaggle_to_raw
```

Configuracion opcional:

```json
{
  "ingestion_date": "2026-05-29",
  "landing_run_id": "airflow_20260529T120000"
}
```

Si no envias configuracion:

- `ingestion_date` usa `{{ ds }}`;
- `landing_run_id` usa el `run_id` real de Airflow.

## Resultado esperado

Al finalizar correctamente:

```text
S3 Landing contiene los 3 CSV originales.
S3 Landing contiene landing_manifest.json.
Airflow registra LANDING_VALIDATION_OK.
Spark escribe Parquet en S3 Raw.
```

Si falta un archivo, el flujo se detiene en `validate_landing_files` y `spark_landing_to_raw` no se ejecuta.
