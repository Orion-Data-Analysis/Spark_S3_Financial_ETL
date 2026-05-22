ara construir la capa raw desde landing, lo más recomendado en tu caso es:

Airflow orquesta
Spark procesa/copia grandes datasets
S3 almacena
Es decir, no elegiría Spark o Airflow como si fueran excluyentes. Lo correcto sería:

Airflow = agenda, control, validación, dependencias, logs
Spark = lectura/escritura de datos grandes
Para tu caso, con IEEE-CIS + Credit Card + PaySim, yo haría:

landing -> raw con Spark, ejecutado por Airflow
La razón principal es PaySim: tiene más de 6 millones de registros. Airflow con Pandas puede funcionar, pero no es lo ideal para datasets grandes. Spark es más defendible como arquitectura Data Lake.

Diferencia entre landing y raw

landing:
- archivos originales descargados de Kaggle
- no se modifican
- formato original CSV
- particionado por source_system, ingestion_date, run_id

raw:
- primera capa gobernada del Data Lake
- conserva datos casi originales
- agrega metadata técnica
- convierte a formato analítico, idealmente Parquet
- particiona para consultas posteriores
Yo haría raw así:

s3://.../dev/financial_crisis/raw/
├── ieee_cis_fraud_detection/
│   ├── train_transaction/
│   ├── train_identity/
│   ├── test_transaction/
│   ├── test_identity/
│   └── sample_submission/
├── credit_card_fraud_detection/
│   └── transactions/
└── paysim/
    └── transactions/
Y cada dataset en formato:

.parquet
Con particiones:

source_system
ingestion_date
run_id
Ejemplo:

raw/paysim/transactions/ingestion_date=2026-05-21/run_id=airflow_001/part-0000.parquet
Pasos recomendados

1. Validar landing con Airflow

Antes de Spark, Airflow valida que existan los archivos esperados:

IEEE-CIS:
- train_transaction.csv
- train_identity.csv
- test_transaction.csv
- test_identity.csv
- sample_submission.csv

Credit Card:
- creditcard.csv

PaySim:
- PS_20174392719_1491204439457_log.csv
2. Ejecutar un Spark job desde Airflow

Airflow lanza Spark con:

SparkSubmitOperator
Ese job recibe parámetros:

--bucket orion-financial-crisis-data-395840094505-us-east-2-an
--env dev
--domain financial_crisis
--ingestion-date 2026-05-21
--run-id airflow_001
3. Spark lee desde landing

Spark lee CSV desde rutas como:

s3://.../landing/source_system=paysim/ingestion_date=2026-05-21/run_id=airflow_001/PS_20174392719_1491204439457_log.csv
4. Spark agrega metadata técnica

A cada DataFrame le agregas columnas:

source_system
source_file_name
landing_path
ingestion_date
run_id
raw_ingestion_time
5. Spark escribe en raw como Parquet

Ejemplo:

s3://.../raw/paysim/transactions/ingestion_date=2026-05-21/run_id=airflow_001/
6. Generar manifest raw

Guarda un manifest:

s3://.../raw/manifests/raw_manifest_ingestion_date=2026-05-21_run_id=airflow_001.json
Con información como:

{
  "layer": "raw",
  "ingestion_date": "2026-05-21",
  "run_id": "airflow_001",
  "sources": [
    {
      "source_system": "paysim",
      "dataset": "transactions",
      "format": "parquet",
      "landing_path": "...",
      "raw_path": "...",
      "status": "loaded"
    }
  ]
}
7. Continuar a staging

La siguiente capa ya no debería leer desde landing, sino desde:

raw/
Y ahí sí empiezas limpieza, estandarización y reglas de calidad.

Arquitectura recomendada

S3 landing
   ↓
Airflow validate_landing_files
   ↓
Airflow SparkSubmitOperator
   ↓
Spark landing_to_raw
   ↓
S3 raw en Parquet
   ↓
Airflow raw_to_staging
Por qué esta opción es la mejor

Opción	Recomendación
Solo Airflow + Pandas	Fácil, pero menos escalable para PaySim
Solo Spark manual	Procesa bien, pero pierdes orquestación
Airflow + Spark	Mejor práctica para Data Lake
Airflow + AWS Glue	También muy buena, pero más infraestructura AWS
Para tu proyecto, el camino más sólido es:

Airflow + SparkSubmitOperator + Spark job landing_to_raw + S3 raw Parquet
Eso te permite decir:

Landing conserva los archivos originales de Kaggle.
Raw convierte esas fuentes a formato analítico Parquet, agrega metadata técnica y deja los datos listos para staging.
Airflow orquesta el proceso y Spark ejecuta la ingesta escalable.