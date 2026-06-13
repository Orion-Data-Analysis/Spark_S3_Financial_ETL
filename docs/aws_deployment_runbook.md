# Guía de Despliegue y Ejecución End-to-End desde Cero (AWS)

Esta guía detalla el paso a paso completo para desplegar, configurar y poner a correr toda la plataforma de datos de **Orion Financial Crisis** en una infraestructura de AWS de manera correcta.

---

## Paso 1: Configuración de AWS S3 e IAM (Seguridad)

### 1.1 Crear el Bucket de S3
1. Inicia sesión en la consola de AWS y ve al servicio de **S3**.
2. Haz clic en **Create bucket** y configúralo con:
   * **Region:** `us-east-2` (Ohio).
   * **Bucket name:** `orion-financial-crisis-data-395840094505-us-east-2-an` (o el nombre único que desees).
   * **Block all public access:** Activado (Checked).
   * **Bucket Versioning:** Enabled.
   * **Default Encryption:** SSE-S3 con **Bucket Key** habilitado.

### 1.2 Crear el IAM Role para las Instancias EC2
1. Ve al servicio de **IAM** y haz clic en **Create policy**.
2. Selecciona la pestaña **JSON** y pega la siguiente política de acceso al bucket (cambia el nombre del bucket si es necesario):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "ListAndLocationDataLake",
         "Effect": "Allow",
         "Action": [
           "s3:ListBucket",
           "s3:GetBucketLocation"
         ],
         "Resource": "arn:aws:s3:::orion-financial-crisis-data-395840094505-us-east-2-an"
       },
       {
         "Sid": "ReadWriteDataLakeObjects",
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:AbortMultipartUpload",
           "s3:ListMultipartUploadParts"
         ],
         "Resource": "arn:aws:s3:::orion-financial-crisis-data-395840094505-us-east-2-an/*"
       }
     ]
   }
   ```
3. Guarda la política como `orion-data-platform-s3-policy`.
4. Haz clic en **Roles > Create role**. Selecciona **AWS service** y **EC2**.
5. Asocia la política `orion-data-platform-s3-policy` y crea el rol con el nombre `orion-data-platform-s3-role`.
6. En la lista de instancias EC2 (Master y Workers), selecciónalas y en **Actions > Security > Modify IAM role**, asígnales el rol `orion-data-platform-s3-role`.

---

## Paso 2: Preparación de la Red y EC2
*(Para más detalles de Security Groups y direccionamiento IP, consulta [aws_infrastructure.md](file:///c:/Users/Duvan/OneDrive/Escritorio/Riwi-Modulo-6/Spark_S3_Financial_ETL/docs/aws_infrastructure.md))*

1. Crea las 6 instancias EC2 necesarias con el OS **Ubuntu Server 24.04 LTS**.
2. Instala Docker y Docker Compose en todas las instancias corriendo:
   ```bash
   sudo apt-get update
   sudo apt-get install -y docker.io docker-compose
   sudo usermod -aG docker $USER
   # Cierra y abre sesión para aplicar permisos de docker
   ```
3. Asegúrate de clonar este repositorio en todas las instancias dentro de la ruta `~/Done-data-platform`.

---

## Paso 3: Configuración de Variables de Entorno (`.env`)

En la raíz del proyecto `~/Done-data-platform` de cada máquina, configura el archivo `.env` según corresponda:

### 3.1 Variables para Airflow / Workers:
```env
FINANCIAL_ENV=dev
FINANCIAL_AWS_REGION=us-east-2
FINANCIAL_S3_BUCKET=orion-financial-crisis-data-395840094505-us-east-2-an
FINANCIAL_S3_DOMAIN=financial_crisis
FINANCIAL_S3_BASE=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
FINANCIAL_INPUT_PATH=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing
FINANCIAL_DATA_LAKE_ROOT=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
SPARK_CONN_ID=spark_standalone
```

### 3.2 Variables para Spark (Master / Workers):
```env
SPARK_IMAGE=orion/spark-s3a:3.5.0
SPARK_NETWORK=spark-net
SPARK_MASTER_BIND_HOST=spark-master
SPARK_MASTER_CONNECT_HOST=<IP_PRIVADA_DE_AIRFLOW_MASTER>
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8082
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_WEBUI_PORT=8081
```

---

## Paso 4: Despliegue de Contenedores (Docker Compose)

### 4.1 En `rabbitmq-server` (Instancia Privada)
Levanta RabbitMQ:
```bash
cd ~/Done-data-platform/architecture/rabbitmq
docker compose up -d
```

### 4.2 En `proxy-server` (Instancia Pública)
Levanta Nginx Proxy Manager:
```bash
cd ~/Done-data-platform/architecture/nginx-proxy-manager
docker compose up -d
```
*Accede al panel de administración en `http://<IP_PUBLICA_PROXY>:81` y configura las redirecciones apuntando a las IPs privadas internas de Airflow, Flower, RabbitMQ y Spark.*

### 4.3 En `airflow-master` (Instancia Privada)
1. Levanta el ecosistema maestro de Airflow:
   ```bash
   cd ~/Done-data-platform/architecture/master
   docker compose up -d --build
   ```
2. Crea la red interna de Spark y levanta Spark Master:
   ```bash
   cd ~/Done-data-platform
   docker network create spark-net 2>/dev/null || true
   docker compose -f architecture/spark_orion/master/docker-compose.master.yml up -d --build
   ```

### 4.4 En `airflow-worker-1`, `2` y `3` (Instancias Privadas)
1. Levanta los Workers de Airflow:
   ```bash
   cd ~/Done-data-platform/architecture/worker
   docker compose up -d --build
   ```
2. Levanta los Workers de Spark:
   ```bash
   cd ~/Done-data-platform
   docker network create spark-net 2>/dev/null || true
   # Asegúrate de actualizar el WORKER_ID (1, 2, 3) en el .env de cada máquina
   docker compose -f architecture/spark_orion/worker/docker-compose.worker.yml up -d --build
   ```

---

## Paso 5: Configuración de Airflow UI (Conexiones y Variables)

Accede a la interfaz web de Airflow a través del dominio configurado en el Proxy y realiza los siguientes ajustes:

### 5.1 Conexión AWS (S3)
1. Ve a **Admin > Connections > +**.
2. **Conn Id:** `aws_orion_s3`
3. **Conn Type:** `Amazon Web Services`
4. **Extra:**
   ```json
   {
     "region_name": "us-east-2"
   }
   ```
   *(No pongas llaves estáticas; Airflow heredará los permisos gracias al IAM Role del EC2).*

### 5.2 Conexión Spark
1. Ve a **Admin > Connections > +**.
2. **Conn Id:** `spark_standalone`
3. **Conn Type:** `Spark`
4. **Host:** `spark://<IP_PRIVADA_DE_AIRFLOW_MASTER>`
5. **Port:** `7077`

### 5.3 Variables de Kaggle
1. Ve a **Admin > Variables > +**.
2. Crea las siguientes variables con tus credenciales de Kaggle:
   * **Key:** `kaggle_username` | **Value:** `tu_usuario_kaggle`
   * **Key:** `kaggle_key` | **Value:** `tu_api_key_de_kaggle`

---

## Paso 6: Ejecución del Pipeline y Verificación

1. Entra a la UI de Airflow, busca el DAG `financial_crisis_kaggle_to_raw` y **actívalo** (toggle a ON).
2. Haz clic en **Trigger DAG** para iniciar la ejecución manual.
3. Puedes realizar seguimiento del flujo en la vista de grafo. El pipeline ejecutará:
   * Descarga y carga en `landing/`
   * Ingesta a Raw en Parquet (`raw/`)
   * Transformación, Limpieza y Cuarentena en Staging (`staging/`)
   * Enriquecimiento analítico y perfiles en Intermediate (`intermediate/`)
   * Métricas y auditoría de reconciliación en Mart (`mart/`)

### 6.1 Validaciones en S3:
Al finalizar correctamente, verifica que existan las siguientes carpetas y archivos en tu bucket de S3:
* **Manifiestos y Auditorías:**
  * `s3://.../dev/financial_crisis/landing/_manifests/.../landing_manifest.json`
  * `s3://.../dev/financial_crisis/raw/_manifests/raw_manifest_...json`
  * `s3://.../dev/financial_crisis/staging/quality/quality_report_...json`
  * `s3://.../dev/financial_crisis/mart/mart_data_reconciliation/` (Verifica que la columna `reconciliation_status` muestre `MATCH`).
* **Datos transformados en Parquet:**
  * Capa Staging: `s3://.../staging/financial_fraud_events/`
  * Capa Intermediate: `s3://.../intermediate/int_customer_risk_profile/` y `s3://.../intermediate/int_transaction_velocity/`
  * Capa Mart: `s3://.../mart/mart_daily_fraud_metrics/` y `s3://.../mart/mart_high_risk_alerts/`

### 6.2 Validaciones en Glue Data Catalog (Athena):
Ingresa a **Amazon Athena** y confirma que se crearon y se pueden consultar las siguientes tablas:
* **Base de datos:** `financial_crisis_dev_intermediate`
  * Tabla `int_customer_risk_profile`
  * Tabla `int_transaction_velocity`
* **Base de datos:** `financial_crisis_dev_mart`
  * Tabla `mart_daily_fraud_metrics`
  * Tabla `mart_high_risk_alerts`
  * Tabla `mart_data_reconciliation`
