# README - Estructura y Administracion del Data Lake en S3

Esta guia explica como montar la estructura final del Data Lake del proyecto en Amazon S3, como conectar Airflow y Spark con S3, como cargar las fuentes en la capa `raw` y como administrar los datos en cada capa del flujo.

El proyecto actualmente usa una ruta local compartida:

```text
/opt/data/s3_data_lake
```

En AWS, esa ruta debe mapearse logicamente a un bucket S3:

```text
s3://orion-financial-crisis-data-lake/
```

## 1. Estructura final recomendada

La estructura recomendada separa los datos por ambiente, dominio, capa y dataset.

```text
s3://orion-financial-crisis-data-lake/
├── dev/
│   └── financial_crisis/
│       ├── landing/
│       │   ├── source_system=digital_accounts/
│       │   ├── source_system=virtual_wallets/
│       │   ├── source_system=international_transfers/
│       │   ├── source_system=online_payments/
│       │   ├── source_system=virtual_cards/
│       │   ├── source_system=consumer_credits/
│       │   ├── source_system=qr_payments/
│       │   ├── source_system=corporate_collections/
│       │   ├── source_system=kyc_validations/
│       │   ├── source_system=security_logs/
│       │   ├── source_system=device_events/
│       │   ├── source_system=banking_apis/
│       │   ├── source_system=allied_merchants/
│       │   └── source_system=external_platforms/
│       ├── raw/
│       │   └── source_system=<source_name>/
│       │       └── ingestion_date=YYYY-MM-DD/
│       │           └── run_id=<airflow_run_id>/
│       ├── staging/
│       │   ├── validated_financial_events/
│       │   │   └── event_date=YYYY-MM-DD/
│       │   └── quality/
│       │       ├── quarantined_events/
│       │       └── quality_report/
│       ├── intermediate/
│       │   ├── modeled_financial_events/
│       │   │   └── event_date=YYYY-MM-DD/
│       │   ├── customer_risk_profile/
│       │   └── source_reconciliation/
│       ├── mart/
│       │   ├── risk_by_country/
│       │   ├── operational_metrics/
│       │   ├── fraud_reporting/
│       │   └── executive_metrics/
│       ├── consumption/
│       │   └── dashboard_payload/
│       ├── manifests/
│       │   └── dag_id=<dag_name>/
│       ├── dbt/
│       │   ├── docs/
│       │   └── artifacts/
│       └── logs/
│           ├── airflow/
│           └── spark/
├── qa/
└── prod/
```

Para el proyecto academico puedes iniciar solo con `dev`. Para una entrega mas completa, deja creados `dev`, `qa` y `prod`.

## 2. Equivalencia con las rutas actuales del proyecto

| Ruta actual en contenedores | Ruta recomendada en S3 |
| --- | --- |
| `/opt/data/input` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/landing/` |
| `/opt/data/generated_sources` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/landing/generated/` |
| `/opt/data/s3_data_lake/raw` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/raw/` |
| `/opt/data/s3_data_lake/staging` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/staging/` |
| `/opt/data/s3_data_lake/intermediate` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/intermediate/` |
| `/opt/data/s3_data_lake/mart` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/mart/` |
| `/opt/data/s3_data_lake/consumption` | `s3://orion-financial-crisis-data-lake/dev/financial_crisis/consumption/` |

## 3. Crear el bucket en AWS S3

1. Entra a la consola de AWS.
2. Ve a **S3 > Create bucket**.
3. Usa un nombre unico, por ejemplo:

```text
orion-financial-crisis-data-lake-<account-id>-us-east-2
```

4. Selecciona la region donde tienes la infraestructura, por ejemplo:

```text
us-east-2
```

5. Activa **Block all public access**.
6. Activa **Bucket Versioning** para protegerte ante sobrescrituras accidentales.
7. Activa cifrado por defecto con **SSE-S3** o **SSE-KMS**.
8. Crea el bucket.

S3 no necesita crear carpetas fisicas; las carpetas son prefijos. Aun asi, puedes crear los prefijos iniciales desde la consola o con AWS CLI.

## 4. Crear la estructura base con AWS CLI

Instala y configura AWS CLI en la maquina desde la que vas a administrar S3:

```bash
aws configure
```

Crea los prefijos base subiendo archivos vacios `.keep`:

```bash
BUCKET=orion-financial-crisis-data-lake-<account-id>-us-east-2
BASE=s3://$BUCKET/dev/financial_crisis

touch .keep
aws s3 cp .keep $BASE/landing/.keep
aws s3 cp .keep $BASE/raw/.keep
aws s3 cp .keep $BASE/staging/.keep
aws s3 cp .keep $BASE/intermediate/.keep
aws s3 cp .keep $BASE/mart/.keep
aws s3 cp .keep $BASE/consumption/.keep
aws s3 cp .keep $BASE/manifests/.keep
aws s3 cp .keep $BASE/dbt/artifacts/.keep
aws s3 cp .keep $BASE/dbt/docs/.keep
aws s3 cp .keep $BASE/logs/airflow/.keep
aws s3 cp .keep $BASE/logs/spark/.keep
```

Crea tambien los prefijos de fuentes:

```bash
for source in \
  digital_accounts \
  virtual_wallets \
  international_transfers \
  online_payments \
  virtual_cards \
  consumer_credits \
  qr_payments \
  corporate_collections \
  kyc_validations \
  security_logs \
  device_events \
  banking_apis \
  allied_merchants \
  external_platforms
do
  aws s3 cp .keep $BASE/landing/source_system=$source/.keep
  aws s3 cp .keep $BASE/raw/source_system=$source/.keep
done
```

## 5. Permisos IAM recomendados

La forma mas segura es usar un **IAM Role** asociado a las instancias EC2 de Airflow Master, Airflow Workers, Spark Master y Spark Workers.

Nombre sugerido:

```text
orion-data-platform-s3-role
```

Politica minima recomendada:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListDataLakeBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::orion-financial-crisis-data-lake-<account-id>-us-east-2"
    },
    {
      "Sid": "ReadWriteFinancialCrisisDataLake",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::orion-financial-crisis-data-lake-<account-id>-us-east-2/*"
    }
  ]
}
```

Si usas **SSE-KMS**, agrega permisos sobre la llave KMS:

```json
{
  "Effect": "Allow",
  "Action": [
    "kms:Encrypt",
    "kms:Decrypt",
    "kms:GenerateDataKey"
  ],
  "Resource": "arn:aws:kms:us-east-2:<account-id>:key/<kms-key-id>"
}
```

## 6. Conexion de Airflow con S3

### Opcion recomendada: conexion por IAM Role

Si tus contenedores corren en EC2 con un IAM Role asociado, no necesitas guardar access keys en Airflow. Solo debes crear una conexion Amazon Web Services.

Desde la interfaz de Airflow:

1. Ve a **Admin > Connections**.
2. Crea una conexion nueva.
3. Usa estos valores:

```text
Connection Id: aws_orion_s3
Connection Type: Amazon Web Services
Extra:
{
  "region_name": "us-east-2"
}
```

Si tambien usas S3 para logs remotos de Airflow:

```text
AIRFLOW__LOGGING__REMOTE_LOGGING=true
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/logs/airflow
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_orion_s3
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=false
```

### Opcion alternativa: conexion con access key

Usala solo si no puedes usar IAM Role.

```text
Connection Id: aws_orion_s3
Connection Type: Amazon Web Services
AWS Access Key ID: <access-key>
AWS Secret Access Key: <secret-key>
Extra:
{
  "region_name": "us-east-2"
}
```

No subas access keys al repositorio ni al archivo `.env`.

## 7. Dependencias necesarias en Airflow

El proyecto debe tener instalado el provider de Amazon:

```text
apache-airflow-providers-amazon
```

Si vas a leer y escribir S3 directamente con pandas tambien necesitas:

```text
s3fs
```

Dependencias sugeridas:

```text
apache-airflow-providers-amazon
pandas
pyarrow
s3fs
boto3
```

## 8. Variables de entorno recomendadas

Agrega estas variables en el `.env` de Airflow Master y Workers:

```text
FINANCIAL_ENV=dev
FINANCIAL_AWS_REGION=us-east-2
FINANCIAL_S3_BUCKET=orion-financial-crisis-data-lake-<account-id>-us-east-2
FINANCIAL_S3_DOMAIN=financial_crisis
FINANCIAL_S3_BASE=s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis

FINANCIAL_INPUT_PATH=s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/landing
FINANCIAL_DATA_LAKE_ROOT=s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis
```

Nota importante: el DAG actual usa `Path` de Python y esta pensado para filesystem local. Para usar `s3://` directamente hay dos caminos:

1. Montar S3 como filesystem con `s3fs-fuse` y mantener rutas tipo `/opt/data`.
2. Ajustar el codigo del DAG para usar `boto3`, `S3Hook`, `s3fs` o Spark `s3a://`.

Para produccion, la opcion mas limpia es adaptar el codigo a S3 nativo.

## 9. Conexion de Spark con S3

Spark debe escribir usando el protocolo `s3a://`.

Ruta base recomendada:

```text
s3a://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis
```

En el `SparkSubmitOperator`, agrega configuraciones como estas:

```python
conf={
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "8",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint": "s3.us-east-2.amazonaws.com",
}
```

Si tu imagen Spark no trae los conectores de Hadoop AWS, debes incluir jars compatibles con tu version de Hadoop:

```text
hadoop-aws
aws-java-sdk-bundle
```

Para Bitnami Spark, valida la version de Hadoop dentro del contenedor antes de seleccionar los jars.

## 10. Carga de fuentes en landing

La capa `landing` recibe archivos originales tal como llegan desde las fuentes. No se corrigen datos en esta capa.

Ejemplo para cargar un archivo CSV:

```bash
aws s3 cp digital_accounts.csv \
  s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/landing/source_system=digital_accounts/ingestion_date=2026-05-21/digital_accounts.csv
```

Ejemplo para subir varias fuentes:

```bash
aws s3 sync ./input \
  s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/landing/manual_upload/ingestion_date=2026-05-21/
```

Formatos permitidos por el proyecto:

```text
.csv
.json
.jsonl
```

## 11. Carga a la capa raw

La capa `raw` es la primera capa administrada por el pipeline. Debe contener los datos ya registrados por ingestion, con metadata minima:

```text
source_system
ingestion_time
raw_file_name
ingestion_date
run_id
```

Estructura recomendada:

```text
raw/
└── source_system=digital_accounts/
    └── ingestion_date=2026-05-21/
        └── run_id=manual__2026-05-21T10-00-00/
            ├── part-0000.parquet
            └── _manifest.json
```

Para el DAG actual, la tarea equivalente es:

```text
ingest_raw_sources
```

Esta tarea debe:

1. Leer archivos desde `landing`.
2. Validar que el formato sea permitido.
3. Agregar metadata de ingestion.
4. Escribir en `raw`.
5. Crear un `_manifest.json` con conteo de filas, columnas, fuente y ruta de salida.

## 12. Procesamiento por capas

### Landing

Objetivo:

```text
Recibir archivos originales sin modificar.
```

Reglas:

- No borrar archivos fuente inmediatamente.
- No transformar columnas.
- Guardar por fuente y fecha de ingestion.
- Mantener trazabilidad del archivo original.

Ejemplo:

```text
landing/source_system=online_payments/ingestion_date=2026-05-21/online_payments.csv
```

### Raw

Objetivo:

```text
Registrar datos ingeridos y trazables.
```

Reglas:

- Conservar la mayor fidelidad posible frente al archivo original.
- Agregar columnas tecnicas de ingestion.
- Guardar manifiestos.
- Preferir Parquet para procesamiento distribuido.

Salidas del proyecto:

```text
raw/source_system=<source_name>/ingestion_date=<date>/run_id=<run_id>/
```

### Staging

Objetivo:

```text
Limpiar, tipar y validar datos.
```

Reglas:

- Convertir tipos de datos.
- Validar montos positivos.
- Validar cliente presente.
- Validar fecha del evento.
- Validar canal permitido.
- Separar datos validos y cuarentena.

Salidas del proyecto:

```text
staging/validated_financial_events/event_date=<date>/
staging/quality/quarantined_events/ingestion_date=<date>/
staging/quality/quality_report/ingestion_date=<date>/
```

### Intermediate

Objetivo:

```text
Integrar fuentes y construir entidades analiticas reutilizables.
```

Reglas:

- Construir `modeled_financial_events`.
- Calcular `risk_score`.
- Calcular `risk_level`.
- Crear `traceability_key`.
- Crear perfiles por cliente.
- Crear conciliacion por fuente.

Salidas del proyecto:

```text
intermediate/modeled_financial_events/event_date=<date>/
intermediate/customer_risk_profile/snapshot_date=<date>/
intermediate/source_reconciliation/ingestion_date=<date>/
```

### Mart

Objetivo:

```text
Crear tablas listas para negocio, auditoria, riesgo y operacion.
```

Reglas:

- Mantener datasets pequenos y consultables.
- Publicar metricas ejecutivas.
- Separar indicadores operacionales, fraude y riesgo.

Salidas del proyecto:

```text
mart/risk_by_country/snapshot_date=<date>/
mart/operational_metrics/snapshot_date=<date>/
mart/fraud_reporting/event_date=<date>/
mart/executive_metrics/snapshot_date=<date>/
```

### Consumption

Objetivo:

```text
Publicar datos finales para dashboards, APIs o reportes.
```

Reglas:

- Guardar payloads livianos.
- No usar esta capa como fuente primaria de reprocesamiento.
- Versionar salidas por fecha o run.

Salida del proyecto:

```text
consumption/dashboard_payload/snapshot_date=<date>/dashboard_payload.json
```

## 13. Administracion de datos

### Nombres y particiones

Usa nombres consistentes:

```text
source_system=<source>
ingestion_date=YYYY-MM-DD
event_date=YYYY-MM-DD
snapshot_date=YYYY-MM-DD
run_id=<airflow_run_id>
```

Usa `ingestion_date` cuando importa cuando llego el dato.
Usa `event_date` cuando importa cuando ocurrio la transaccion.
Usa `snapshot_date` para resultados agregados o metricas de cierre.

### Manifiestos

Cada ejecucion debe generar un manifiesto:

```text
manifests/dag_id=digital_financial_crisis_full_flow/run_id=<run_id>/_manifest.json
```

Contenido recomendado:

```json
{
  "dag_id": "digital_financial_crisis_full_flow",
  "run_id": "manual__2026-05-21T10:00:00",
  "created_at": "2026-05-21T10:05:00Z",
  "source_files": [],
  "outputs": [],
  "row_counts": {},
  "quality_report": {},
  "status": "success"
}
```

### Calidad

La calidad se administra en:

```text
staging/quality/
```

Debe incluir:

- registros en cuarentena;
- reporte de calidad;
- conteo de duplicados;
- conteo de montos invalidos;
- conteo de clientes faltantes;
- conteo de fechas invalidas;
- conteo de canales invalidos.

### Retencion

Recomendacion inicial:

| Capa | Retencion sugerida |
| --- | --- |
| `landing` | 30 a 90 dias |
| `raw` | 1 a 5 anos |
| `staging` | 6 a 24 meses |
| `intermediate` | 6 a 24 meses |
| `mart` | 1 a 5 anos |
| `consumption` | 30 a 180 dias |
| `logs` | 30 a 90 dias |

Puedes automatizar esto con **S3 Lifecycle Rules**.

### Seguridad

Recomendaciones:

- Bloquear acceso publico al bucket.
- Usar IAM Roles en EC2.
- Evitar access keys en archivos `.env`.
- Cifrar datos con SSE-S3 o SSE-KMS.
- Separar permisos por ambiente (`dev`, `qa`, `prod`).
- Activar versionamiento en el bucket.
- Activar CloudTrail para auditoria de acceso a S3.

## 14. Flujo operacional completo

El flujo diario recomendado es:

```text
1. Cargar archivos fuente en landing.
2. Airflow ejecuta ingestion hacia raw.
3. Spark o Pandas procesa raw hacia staging.
4. Staging separa validos y cuarentena.
5. Intermediate integra eventos y calcula riesgo.
6. Mart genera vistas de negocio.
7. Consumption publica payloads para dashboards.
8. Manifests registra trazabilidad de la ejecucion.
9. Logs quedan en S3 para auditoria.
```

## 15. Validaciones despues de montar S3

Desde una instancia EC2 con el IAM Role:

```bash
aws sts get-caller-identity
aws s3 ls s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/
```

Prueba escritura:

```bash
echo "s3 ok" > s3_test.txt
aws s3 cp s3_test.txt s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/_checks/s3_test.txt
aws s3 rm s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/_checks/s3_test.txt
```

Prueba desde Airflow con un DAG o una tarea Bash:

```bash
aws s3 ls s3://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/
```

Prueba desde Spark:

```python
spark.read.csv(
    "s3a://orion-financial-crisis-data-lake-<account-id>-us-east-2/dev/financial_crisis/landing/",
    header=True
)
```

## 16. Recomendacion para evolucionar el codigo actual

El codigo actual funciona bien sobre filesystem local o EFS. Para trabajar directamente con S3 se recomienda:

1. Cambiar escritura CSV local por escritura Parquet en S3.
2. Usar `s3a://` en el job Spark.
3. Usar `S3Hook` o `boto3` en tareas Airflow que no son Spark.
4. Guardar manifiestos por `dag_id` y `run_id`.
5. Mantener `raw`, `staging`, `intermediate`, `mart` y `consumption` como capas logicas estables.

La prioridad de implementacion sugerida es:

```text
1. Crear bucket y estructura.
2. Crear IAM Role y probar permisos.
3. Configurar logs remotos de Airflow en S3.
4. Cargar fuentes en landing.
5. Adaptar ingestion raw.
6. Adaptar Spark a s3a://.
7. Adaptar dbt o Athena/Glue para leer marts.
```

