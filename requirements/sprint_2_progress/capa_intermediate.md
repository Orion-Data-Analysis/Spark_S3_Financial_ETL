Sí: después de staging, la capa intermediate debería construir datos integrados, enriquecidos y reutilizables para análisis, modelos y marts.

Mi recomendación para tu caso:

staging -> intermediate: Spark
intermediate -> mart: dbt + Athena
Athena no es estrictamente necesaria para construir intermediate si ya estás usando Spark, pero sí te conviene empezar a registrar tablas externas desde staging o intermediate en Glue Catalog para que dbt/Athena puedan leerlas después.

Rol de cada herramienta

Spark:
- procesa grandes volúmenes
- integra datasets
- calcula features pesadas
- escribe Parquet en S3

dbt:
- modela SQL
- crea vistas/tablas analíticas
- documenta lineage
- prueba reglas de negocio

Athena:
- motor SQL sobre S3
- ejecuta modelos dbt
- consulta tablas externas
Para intermediate, yo haría principalmente Spark y dejaría dbt/Athena para mart.

Objetivo de intermediate

Desde:

staging/financial_fraud_events/
Construyes datasets como:

intermediate/
├── int_financial_events_enriched/
├── int_customer_risk_profile/
├── int_transaction_velocity/
├── int_source_reconciliation/
├── int_fraud_patterns/
├── int_model_features/
└── quality/
Pasos detallados

1. Validar que staging esté completo

Airflow debe validar que existan:

staging/financial_fraud_events/
staging/paysim/transactions/
staging/credit_card_fraud_detection/transactions/
staging/ieee_cis_fraud_detection/transactions/
staging/quality/quality_report.json
2. Registrar staging en Glue Catalog

Esto es importante si después usarás Athena/dbt.

Puedes hacerlo con Glue Crawler o con Spark SQL.

Ejemplo de base de datos:

financial_crisis_dev_staging
financial_crisis_dev_intermediate
financial_crisis_dev_mart
Tablas:

financial_fraud_events
paysim_transactions
credit_card_transactions
ieee_cis_transactions
Esto permite consultar staging desde Athena:

SELECT source_system, count(*)
FROM financial_crisis_dev_staging.financial_fraud_events
GROUP BY source_system;
3. Crear Spark job staging_to_intermediate

Archivo sugerido:

architecture/pipelines/spark_jobs/staging_to_intermediate_financial_crisis.py
Debe recibir:

--bucket orion-financial-crisis-data-395840094505-us-east-2-an
--env dev
--domain financial_crisis
--ingestion-date 2026-05-21
--run-id airflow_001
4. Leer staging unificado

events = spark.read.parquet(
    f"s3a://{bucket}/{env}/{domain}/staging/financial_fraud_events/"
    f"ingestion_date={ingestion_date}/run_id={run_id}/"
)
Filtra solo válidos:

valid_events = events.filter("quality_status = 'valid'")
5. Crear eventos enriquecidos

Dataset:

intermediate/int_financial_events_enriched/
Aquí agregas variables útiles:

amount_bucket
risk_amount_level
is_high_amount
event_date
event_hour
event_day_of_week
source_priority
fraud_label_normalized
transaction_family
Ejemplos:

amount_bucket:
0-100
100-500
500-1000
1000+

risk_amount_level:
low
medium
high
critical
6. Crear perfil de riesgo por cliente/cuenta

Dataset:

intermediate/int_customer_risk_profile/
Agrupa por:

customer_id
origin_account_id
source_system
Métricas:

total_events
fraud_events
fraud_rate
total_amount
avg_amount
max_amount
first_event_time
last_event_time
high_amount_events
sources_observed
Si algunas fuentes no tienen customer_id, usa:

customer_id
origin_account_id
card_id
synthetic_entity_id
7. Crear velocidad transaccional

Dataset:

intermediate/int_transaction_velocity/
Métricas por entidad y ventana:

events_last_1h
events_last_24h
amount_last_1h
amount_last_24h
fraud_events_last_24h
Con Spark puedes usar window functions.

Esto es muy útil para fraude.

8. Crear patrones de fraude

Dataset:

intermediate/int_fraud_patterns/
Agrupa por:

source_system
transaction_type
amount_bucket
channel
event_hour
Métricas:

events
fraud_events
fraud_rate
avg_amount
max_amount
9. Crear conciliación por fuente

Dataset:

intermediate/int_source_reconciliation/
Compara:

input_rows_staging
valid_rows
quarantine_rows
fraud_rows
non_fraud_rows
Por:

source_system
raw_dataset
ingestion_date
run_id
Esto sirve mucho para trazabilidad.

10. Crear features para modelos

Dataset:

intermediate/int_model_features/
Columnas sugeridas:

event_id
source_system
amount
amount_bucket_index
event_hour
event_day_of_week
transaction_type_index
fraud_rate_by_source
customer_fraud_rate
customer_avg_amount
is_high_amount
is_cross_border
is_fraud
Esta tabla es útil si luego quieres entrenar modelos ML o alimentar análisis.

11. Escribir intermediate en Parquet

Cada salida:

s3://.../dev/financial_crisis/intermediate/int_financial_events_enriched/ingestion_date=.../run_id=.../
s3://.../dev/financial_crisis/intermediate/int_customer_risk_profile/ingestion_date=.../run_id=.../
s3://.../dev/financial_crisis/intermediate/int_transaction_velocity/ingestion_date=.../run_id=.../
s3://.../dev/financial_crisis/intermediate/int_fraud_patterns/ingestion_date=.../run_id=.../
s3://.../dev/financial_crisis/intermediate/int_source_reconciliation/ingestion_date=.../run_id=.../
s3://.../dev/financial_crisis/intermediate/int_model_features/ingestion_date=.../run_id=.../
12. Generar manifest de intermediate

intermediate/manifests/intermediate_manifest.json
Con:

{
  "layer": "intermediate",
  "ingestion_date": "2026-05-21",
  "run_id": "airflow_001",
  "datasets": [
    "int_financial_events_enriched",
    "int_customer_risk_profile",
    "int_transaction_velocity",
    "int_fraud_patterns",
    "int_source_reconciliation",
    "int_model_features"
  ]
}
13. Registrar intermediate en Glue Catalog

Después de escribir Parquet, registra tablas externas para Athena/dbt:

financial_crisis_dev_intermediate.int_financial_events_enriched
financial_crisis_dev_intermediate.int_customer_risk_profile
financial_crisis_dev_intermediate.int_transaction_velocity
financial_crisis_dev_intermediate.int_fraud_patterns
financial_crisis_dev_intermediate.int_source_reconciliation
financial_crisis_dev_intermediate.int_model_features
Aquí Athena empieza a tener sentido.

14. Orquestar con Airflow

Flujo:

validate_staging_outputs
  -> spark_staging_to_intermediate
  -> register_intermediate_tables
  -> validate_intermediate_outputs
Ejemplo con SparkSubmitOperator:

spark_staging_to_intermediate = SparkSubmitOperator(
    task_id="spark_staging_to_intermediate",
    application="/opt/pipelines/spark_jobs/staging_to_intermediate_financial_crisis.py",
    conn_id="spark_standalone",
    application_args=[
        "--bucket", "{{ var.value.financial_crisis_bucket }}",
        "--env", "{{ var.value.financial_crisis_env }}",
        "--domain", "{{ var.value.financial_crisis_domain }}",
        "--ingestion-date", "{{ ds }}",
        "--run-id", "{{ run_id }}",
    ],
)
15. ¿Dónde entra dbt?

Yo usaría dbt después de intermediate, para construir mart.

Ejemplo:

intermediate -> dbt -> mart
dbt leería tablas Athena del esquema:

financial_crisis_dev_intermediate
Y crearía:

financial_crisis_dev_mart.fct_fraud_events
financial_crisis_dev_mart.dim_source_system
financial_crisis_dev_mart.dim_risk_profile
financial_crisis_dev_mart.agg_fraud_by_source
financial_crisis_dev_mart.agg_fraud_by_day
financial_crisis_dev_mart.kpi_fraud_summary
Respuesta corta sobre Athena

¿Necesitas Athena en intermediate?

Para procesar: no, Spark puede hacerlo.
Para consultar/registrar/usar dbt: sí, conviene desde intermediate.
La combinación ideal:

staging -> intermediate:
Spark escribe Parquet en S3

intermediate:
Glue Catalog registra tablas externas

mart:
dbt ejecuta SQL sobre Athena leyendo intermediate
Así tu arquitectura queda muy fuerte:

landing: originales Kaggle
raw: parquet con metadata técnica
staging: limpio y estandarizado
intermediate: integrado, enriquecido, features y perfiles
mart: modelo dimensional/KPIs con dbt + Athena
consumption: dashboards/reportes

typo de modelo:
Sí, para tu caso usaría un modelo estrella en la capa mart.

La capa intermediate prepara datos enriquecidos; la capa mart debería quedar lista para analítica, dashboards y consultas de negocio. Ahí el modelo estrella encaja muy bien.

El centro sería una tabla de hechos:

fct_fraud_events
Y alrededor varias dimensiones:

dim_date
dim_source_system
dim_transaction_type
dim_customer
dim_account
dim_risk_level
dim_channel
dim_geography
Modelo propuesto:

                 dim_date
                    |
dim_source_system - fct_fraud_events - dim_transaction_type
                    |
              dim_customer
                    |
               dim_account
                    |
              dim_risk_level
                    |
                dim_channel
                    |
              dim_geography
Tabla de hechos principal

fct_fraud_events
Grano recomendado:

1 fila = 1 evento/transacción financiera estandarizada
Columnas:

event_id
transaction_id
date_key
source_system_key
transaction_type_key
customer_key
account_key
risk_level_key
channel_key
geography_key
amount
is_fraud
is_flagged_fraud
risk_score
event_count
ingestion_date
run_id
Medidas:

amount
risk_score
event_count
is_fraud
is_flagged_fraud
Dimensiones recomendadas

dim_date
Para análisis por día, mes, trimestre, año:

date_key
date
year
month
month_name
quarter
day
day_of_week
is_weekend
dim_source_system
Para separar Kaggle/PaySim/IEEE/Credit Card:

source_system_key
source_system
source_dataset
source_type
provider
dim_transaction_type
transaction_type_key
transaction_type
transaction_family
dim_customer
Puede ser parcial porque no todas las fuentes tienen cliente real:

customer_key
customer_id
customer_type
is_synthetic
dim_account
account_key
origin_account_id
destination_account_id
account_type
dim_risk_level
risk_level_key
risk_level
risk_score_min
risk_score_max
risk_description
dim_channel
channel_key
channel
channel_group
dim_geography
geography_key
country
region
Tablas agregadas para dashboards

Además del modelo estrella, puedes crear agregados:

agg_fraud_by_source_day
agg_fraud_by_transaction_type
agg_fraud_by_risk_level
agg_fraud_by_country
kpi_fraud_summary
Ejemplo:

agg_fraud_by_source_day
date_key
source_system_key
total_events
fraud_events
fraud_rate
total_amount
fraud_amount
avg_risk_score
Dónde construirlo

Lo construiría con:

dbt + Athena
Leyendo desde:

intermediate/int_financial_events_enriched
intermediate/int_customer_risk_profile
intermediate/int_fraud_patterns
Y escribiendo en:

mart/
En resumen:

staging: datos limpios
intermediate: datos enriquecidos/features
mart: modelo estrella
Para tu caso, el modelo estrella más importante sería:

fct_fraud_events
+ dim_date
+ dim_source_system
+ dim_transaction_type
+ dim_customer
+ dim_account
+ dim_risk_level
+ dim_channel
Athena + dbt es una muy buena combinación para construir esa capa.