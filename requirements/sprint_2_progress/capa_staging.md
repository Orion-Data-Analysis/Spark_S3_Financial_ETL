Para hacer raw -> staging automatizado con Spark, la idea es que raw tenga los datos casi originales en Parquet y que staging sea la primera capa limpia, tipada, validada y estandarizada.

El flujo recomendado sería:

S3 raw parquet
  -> Spark job raw_to_staging
  -> validaciones de calidad
  -> datasets staging por fuente
  -> dataset staging unificado
  -> cuarentena
  -> quality_report.json
1. Definir entrada raw

Tu capa raw debería tener algo así:

s3://.../dev/financial_crisis/raw/
├── ieee_cis_fraud_detection/
│   ├── train_transaction/
│   ├── train_identity/
│   ├── test_transaction/
│   └── test_identity/
├── credit_card_fraud_detection/
│   └── transactions/
└── paysim/
    └── transactions/
Cada dataset en Parquet:

raw/paysim/transactions/ingestion_date=2026-05-21/run_id=airflow_001/
2. Definir salida staging

Yo usaría esta estructura:

s3://.../dev/financial_crisis/staging/
├── ieee_cis_fraud_detection/
│   └── transactions/
├── credit_card_fraud_detection/
│   └── transactions/
├── paysim/
│   └── transactions/
├── financial_fraud_events/
├── quality/
│   ├── quarantined_events/
│   └── quality_report.json
└── _manifest.json
3. Crear un Spark job raw_to_staging

Archivo sugerido:

architecture/pipelines/spark_jobs/raw_to_staging_financial_crisis.py
Este job debe recibir parámetros:

--bucket orion-financial-crisis-data-395840094505-us-east-2-an
--env dev
--domain financial_crisis
--ingestion-date 2026-05-21
--run-id airflow_001
4. Leer datos raw con Spark

Spark lee Parquet:

paysim_df = spark.read.parquet(
    f"s3a://{bucket}/{env}/{domain}/raw/paysim/transactions/"
    f"ingestion_date={ingestion_date}/run_id={run_id}/"
)

credit_df = spark.read.parquet(
    f"s3a://{bucket}/{env}/{domain}/raw/credit_card_fraud_detection/transactions/"
    f"ingestion_date={ingestion_date}/run_id={run_id}/"
)

ieee_tx_df = spark.read.parquet(
    f"s3a://{bucket}/{env}/{domain}/raw/ieee_cis_fraud_detection/train_transaction/"
    f"ingestion_date={ingestion_date}/run_id={run_id}/"
)
5. Limpiar y tipar PaySim

PaySim puede mapearse fácilmente a un esquema de eventos:

step -> event_step
type -> transaction_type
amount -> amount
nameOrig -> origin_account_id
nameDest -> destination_account_id
oldbalanceOrg -> origin_old_balance
newbalanceOrig -> origin_new_balance
oldbalanceDest -> destination_old_balance
newbalanceDest -> destination_new_balance
isFraud -> is_fraud
isFlaggedFraud -> is_flagged_fraud
Validaciones:

amount > 0
type no nulo
nameOrig no nulo
nameDest no nulo
isFraud en 0/1
Salida staging:

staging/paysim/transactions/
6. Limpiar y tipar Credit Card Fraud

Credit Card tiene columnas anonimizadas:

Time
V1 ... V28
Amount
Class
Mapeo sugerido:

Time -> event_time_offset_seconds
Amount -> amount
Class -> is_fraud
V1..V28 -> feature_v1..feature_v28
Validaciones:

amount >= 0
Class en 0/1
Time no nulo
Salida staging:

staging/credit_card_fraud_detection/transactions/
7. Limpiar y tipar IEEE-CIS

IEEE-CIS es más complejo porque tiene:

train_transaction.csv
train_identity.csv
test_transaction.csv
test_identity.csv
En staging deberías unir:

train_transaction + train_identity por TransactionID
Para entrenamiento/analítica, train es el dataset útil porque tiene isFraud.

Mapeo base:

TransactionID -> transaction_id
TransactionDT -> event_time_offset_seconds
TransactionAmt -> amount
ProductCD -> product_code
card1..card6 -> card_*
addr1, addr2 -> address_*
P_emaildomain -> payer_email_domain
R_emaildomain -> receiver_email_domain
isFraud -> is_fraud
Validaciones:

TransactionID no nulo
TransactionAmt > 0
isFraud en 0/1
Salida staging:

staging/ieee_cis_fraud_detection/transactions/
8. Crear dataset unificado

Después de limpiar por fuente, crea una tabla común:

staging/financial_fraud_events/
Con columnas estándar:

event_id
source_system
transaction_id
event_time
event_time_offset_seconds
amount
currency
transaction_type
channel
customer_id
origin_account_id
destination_account_id
country
is_fraud
is_flagged_fraud
raw_dataset
ingestion_date
run_id
staging_processed_at
quality_status
quality_errors
No todas las fuentes tendrán todas las columnas. Usa null donde no aplique.

9. Separar válidos y cuarentena

En staging, separa:

quality_status = valid
quality_status = quarantine
Errores típicos:

missing_transaction_id
invalid_amount
missing_fraud_label
invalid_fraud_label
missing_event_time
Salida:

staging/financial_fraud_events/
staging/quality/quarantined_events/
10. Generar reporte de calidad

Crear:

staging/quality/quality_report.json
Debe incluir:

{
  "layer": "staging",
  "ingestion_date": "2026-05-21",
  "run_id": "airflow_001",
  "sources": [
    {
      "source_system": "paysim",
      "input_rows": 6362620,
      "valid_rows": 6362620,
      "quarantine_rows": 0
    }
  ]
}
11. Orquestarlo con Airflow

En tu DAG:

validate_raw_files
  -> spark_raw_to_staging
  -> validate_staging_outputs
  -> intermediate_layer
Con SparkSubmitOperator:

spark_raw_to_staging = SparkSubmitOperator(
    task_id="spark_raw_to_staging",
    application="/opt/pipelines/spark_jobs/raw_to_staging_financial_crisis.py",
    conn_id="spark_standalone",
    application_args=[
        "--bucket", "{{ var.value.financial_crisis_bucket }}",
        "--env", "{{ var.value.financial_crisis_env }}",
        "--domain", "{{ var.value.financial_crisis_domain }}",
        "--ingestion-date", "{{ ds }}",
        "--run-id", "{{ run_id }}",
    ],
)
12. Orden recomendado de implementación

Implementa primero raw_to_staging solo para PaySim.
Luego agrega Credit Card.
Después IEEE-CIS, porque requiere joins.
Finalmente crea financial_fraud_events unificado.
Agrega cuarentena y quality_report.json.
Conecta el job al DAG de Airflow.
Mi recomendación concreta

Para staging automatizado, usa:

Spark para transformar y validar
Airflow para ejecutar y controlar
S3 para almacenar outputs
Parquet para staging
Así tu arquitectura queda clara:

landing: archivos originales de Kaggle
raw: parquet gobernado con metadata técnica
staging: datos limpios, tipados, validados y estandarizados