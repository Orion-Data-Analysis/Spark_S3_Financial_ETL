# Runbook Airflow: Kaggle -> Landing -> Raw

## Estado implementado

Se implementaron dos DAGs en Airflow:

```text
financial_crisis_kaggle_to_raw
validate_financial_landing_files
```

El DAG principal es:

```text
financial_crisis_kaggle_to_raw
```

El DAG `validate_financial_landing_files` queda como validacion independiente para auditar Landing cuando los archivos ya existen en S3.

## Que hace cada DAG

### financial_crisis_kaggle_to_raw

Ejecuta el flujo completo:

```text
Kaggle
  -> descarga datasets
  -> descomprime archivos
  -> sube CSV originales a S3 Landing
  -> genera manifest de Landing
  -> valida archivos de Landing
  -> ejecuta Spark
  -> escribe Raw en Parquet
```

Orden interno:

```text
build_landing_context
  -> configure_kaggle_credentials
  -> download_unzip_upload_sources_to_landing
  -> generate_landing_manifest
  -> validate_landing_files
  -> spark_landing_to_raw
```

La tarea `download_unzip_upload_sources_to_landing` agrupa descarga, descompresion y carga a S3 en el mismo worker Celery. Esto evita errores por rutas locales `/tmp` o credenciales temporales que no existen en otros workers.

### validate_financial_landing_files

Solo valida Landing:

```text
S3 Landing
  -> verifica existencia de los 3 CSV esperados
  -> verifica que no esten vacios
  -> falla si falta algo
```

Este DAG no descarga Kaggle ni ejecuta Spark.

## Archivos creados o modificados

```text
architecture/pipelines/dags/financial_crisis_kaggle_to_raw.py
architecture/pipelines/dags/validate_financial_landing_files.py
architecture/pipelines/dags/common/financial_landing.py
architecture/pipelines/spark_jobs/landing_to_raw_financial_crisis.py
architecture/Dockerfile
docs/kaggle_to_raw_financial_pipeline.md
docs/landing_validation_financial_files.md
docs/airflow_kaggle_to_raw_runbook.md
requirements/sprint_2_progress/capa _raw.md
```

## Por que common esta dentro de dags

Airflow estaba leyendo los DAGs desde:

```text
/opt/pipelines/dags
```

El primer intento dejo el helper en:

```text
/opt/pipelines/common
```

Eso produjo:

```text
ModuleNotFoundError: No module named 'common'
```

La solucion fue mover el helper compartido a:

```text
architecture/pipelines/dags/common/financial_landing.py
```

Asi el import funciona desde los DAGs:

```python
from common.financial_landing import validate_expected_landing_files
```

## Configuracion requerida en Airflow

### Variable Kaggle

En Airflow UI:

```text
Admin -> Variables -> +
```

Crear:

```text
Key: kaggle_api_token
Value: <token KGAT generado por Kaggle>
```

Si el token fue compartido en una captura o chat, revocarlo y crear uno nuevo.

Tambien existe soporte para el formato legado:

```text
kaggle_username
kaggle_key
```

Pero para el token nuevo de Kaggle basta con:

```text
kaggle_api_token
```

### Aceptar reglas IEEE-CIS

Antes de ejecutar el DAG, entrar con la misma cuenta de Kaggle y aceptar las reglas:

```text
https://www.kaggle.com/competitions/ieee-fraud-detection/data
```

Si no se aceptan, la tarea `download_unzip_upload_sources_to_landing` puede fallar aunque el token sea valido.

### Conexion AWS

Debe existir:

```text
Connection Id: aws_orion_s3
Connection Type: Amazon Web Services
Region: us-east-2
```

En EC2 se recomienda dejar Access Key y Secret Key vacios y usar IAM Role.

### Conexion Spark

Debe existir:

```text
Connection Id: spark_standalone
Connection Type: Spark
Host: spark://10.0.2.251
Port: 7077
```

Si Airflow no acepta `spark://` en Host:

```text
Host: 10.0.2.251
Port: 7077
Extra: {"deploy-mode": "client"}
```

## Despliegue en EC2

Los cambios deben estar en:

```text
Airflow Master: obligatorio
Airflow Workers: obligatorio
Spark Master: recomendado
Spark Workers: recomendado
```

Actualizar codigo:

```bash
cd ~/orion_caso4
git pull
```

Si solo cambia codigo de DAGs o jobs montados por volumen:

```bash
cd architecture/master
docker compose up -d
```

En workers:

```bash
cd architecture/worker
docker compose up -d
```

Si cambia el Dockerfile o dependencias, reconstruir imagen:

```bash
cd architecture/master
docker compose build
docker compose up -d
```

En workers:

```bash
cd architecture/worker
docker compose build
docker compose up -d
```

## Validaciones antes de ejecutar

Desde Airflow master:

```bash
cd ~/orion_caso4/architecture/master
docker compose run --rm airflow-cli dags list-import-errors
```

Resultado esperado:

```text
No data found
```

Listar DAGs:

```bash
docker compose run --rm airflow-cli dags list | grep financial
```

Resultado esperado:

```text
financial_crisis_kaggle_to_raw
validate_financial_landing_files
```

Si aparecen warnings de `PYTHONPATH` o `EXTRA_REQUIREMENTS`, no bloquean la ejecucion. Para reducirlos se puede ejecutar con:

```bash
docker compose --env-file ../.env run --rm airflow-cli dags list
```

## Ejecucion del DAG principal

En Airflow UI:

```text
DAGs -> financial_crisis_kaggle_to_raw -> Trigger DAG
```

Usar este JSON en `Configuracion JSON`:

```json
{
  "ingestion_date": "2026-05-29",
  "landing_run_id": "airflow_20260529T120000"
}
```

Campos que pueden quedar vacios:

```text
ID de la ejecucion
Partition key
Nota de ejecucion
```

Tambien se puede ejecutar por CLI:

```bash
cd ~/orion_caso4/architecture/master
docker compose --env-file ../.env run --rm airflow-cli dags trigger financial_crisis_kaggle_to_raw --conf '{"ingestion_date":"2026-05-29","landing_run_id":"airflow_20260529T120000"}'
```

## Rutas generadas

Landing:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=<source_system>/ingestion_date=2026-05-29/run_id=airflow_20260529T120000/<file_name>
```

Manifest:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/_manifests/ingestion_date=2026-05-29/run_id=airflow_20260529T120000/landing_manifest.json
```

Raw:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/raw/<source_system>/<dataset>/ingestion_date=2026-05-29/run_id=airflow_20260529T120000/
```

## Errores comunes

### ModuleNotFoundError: No module named 'common'

Verificar que exista:

```text
/opt/pipelines/dags/common/financial_landing.py
```

Luego reiniciar Airflow:

```bash
cd architecture/master
docker compose up -d
```

### Falla download_unzip_upload_sources_to_landing

Revisar:

```text
kaggle_api_token existe en Airflow Variables
la cuenta acepto reglas de IEEE-CIS
el token no fue revocado
el contenedor tiene instalado kaggle
```

Tambien revisar conectividad a internet desde los workers.

### Falla upload dentro de download_unzip_upload_sources_to_landing

Revisar:

```text
aws_orion_s3 existe
IAM Role tiene permisos S3
bucket correcto
region us-east-2
```

### Falla validate_landing_files

Revisar en logs:

```text
source_system
file_name
s3_uri
```

La tarea indica exactamente que archivo falta o esta vacio.

### Falla spark_landing_to_raw

Revisar:

```text
spark_standalone existe
Spark Master esta activo en 10.0.2.251:7077
Airflow puede conectarse al puerto 7077
Spark tiene acceso a S3 por IAM Role
```
