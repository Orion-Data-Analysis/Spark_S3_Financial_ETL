# Validacion de archivos financieros en S3 Landing

## Objetivo

Antes de iniciar el procesamiento Spark hacia la capa Raw, Airflow debe validar que los archivos financieros crudos esperados existan en S3 Landing y que no esten vacios. Si falta un archivo o tiene tamano cero, el DAG falla y deja un mensaje descriptivo en los logs para auditoria.

La implementacion quedo en:

```text
architecture/pipelines/dags/validate_financial_landing_files.py
architecture/pipelines/common/financial_landing.py
```

Tambien se reutiliza dentro del flujo completo:

```text
architecture/pipelines/dags/financial_crisis_kaggle_to_raw.py
```

## Archivos obligatorios

| Source system | Dataset | Archivo |
|---|---|---|
| `ieee_cis_fraud_detection` | `train_transaction` | `train_transaction.csv` |
| `credit_card_fraud_detection` | `transactions` | `creditcard.csv` |
| `paysim` | `transactions` | `PS_20174392719_1491204439457_log.csv` |

## Contrato de rutas

La capa Landing usa esta convencion:

```text
s3://<bucket>/<env>/<domain>/landing/source_system=<source_system>/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/<file_name>
```

Con los valores actuales del proyecto:

```text
bucket = orion-financial-crisis-data-395840094505-us-east-2-an
env    = dev
domain = financial_crisis
```

Rutas esperadas:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=ieee_cis_fraud_detection/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/train_transaction.csv

s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=credit_card_fraud_detection/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/creditcard.csv

s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing/source_system=paysim/ingestion_date=<YYYY-MM-DD>/run_id=<run_id>/PS_20174392719_1491204439457_log.csv
```

## Como funciona

El DAG `validate_financial_landing_files` ejecuta la tarea `validate_landing_files` con `PythonOperator`.

La tarea:

1. Lee `ingestion_date` y `landing_run_id` desde `dag_run.conf`.
2. Si no se reciben parametros, usa `{{ ds }}` y el `run_id` de la ejecucion actual.
3. Construye las tres rutas esperadas en S3 Landing.
4. Valida existencia con `S3Hook.check_for_key`.
5. Valida completitud minima con `head_object` y `ContentLength > 0`.
6. Falla con `AirflowFailException` si hay archivos faltantes o vacios.
7. Registra en logs el `source_system`, `file_name` y `s3_uri` afectado.

## Configuracion de produccion

La tarea esta configurada con:

```text
retries = 3
retry_delay = 5 minutos
retry_exponential_backoff = True
max_retry_delay = 20 minutos
execution_timeout = 15 minutos
```

Esto permite tolerar retrasos temporales de S3 o de la carga Landing sin permitir que una ejecucion quede esperando indefinidamente.

## Variables usadas

El DAG lee estas variables de entorno, con valores por defecto alineados al proyecto:

| Variable | Valor por defecto |
|---|---|
| `FINANCIAL_AWS_CONN_ID` | `aws_orion_s3` |
| `FINANCIAL_S3_BUCKET` | `orion-financial-crisis-data-395840094505-us-east-2-an` |
| `FINANCIAL_ENV` | `dev` |
| `FINANCIAL_S3_DOMAIN` | `financial_crisis` |

La conexion `aws_orion_s3` ya esta definida en el `.env` para usar IAM Role de EC2:

```text
AIRFLOW_CONN_AWS_ORION_S3={"conn_type":"aws","extra":{"region_name":"us-east-2"}}
```

## Ejecucion manual

Desde Airflow UI se puede disparar el DAG con configuracion:

```json
{
  "ingestion_date": "2026-05-29",
  "landing_run_id": "airflow_20260529T120000"
}
```

Si el pipeline de extraccion usa el mismo `run_id` de Airflow, tambien se puede enviar:

```json
{
  "ingestion_date": "2026-05-29",
  "run_id": "manual__2026-05-29T12:00:00+00:00"
}
```

## Logs esperados

Cuando falta un archivo:

```text
[LANDING_VALIDATION_ERROR] Missing financial file in S3 Landing. source_system=paysim file_name=PS_20174392719_1491204439457_log.csv s3_uri=s3://...
```

Cuando un archivo existe pero esta vacio:

```text
[LANDING_VALIDATION_ERROR] Empty financial file in S3 Landing. source_system=credit_card_fraud_detection file_name=creditcard.csv s3_uri=s3://... size_bytes=0
```

Cuando todo esta correcto:

```text
[LANDING_VALIDATION_OK] All expected financial files exist and are not empty.
```

## Integracion con el pipeline Raw

Esta validacion debe ejecutarse antes del job Spark `landing_to_raw`:

```text
validate_landing_files
  -> spark_landing_to_raw
  -> raw_to_staging
```

Si se integra dentro de un DAG principal existente, se puede reutilizar la funcion `validate_landing_files` o copiar la tarea `PythonOperator` antes del `SparkSubmitOperator`.

## Criterios de aceptacion cubiertos

| Criterio | Estado |
|---|---|
| Documentar los 3 archivos financieros esperados | Cumplido |
| Definir rutas por `source_system`, `ingestion_date` y `run_id` | Cumplido |
| Verificar existencia en S3 Landing antes de Spark | Cumplido |
| Validar completitud minima del archivo | Cumplido |
| Detener el pipeline si hay faltantes o archivos vacios | Cumplido |
| Registrar mensajes descriptivos para trazabilidad y auditoria | Cumplido |
