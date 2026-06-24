# Guía de ejecución — Orion Financial Crisis Data Platform

Generada a partir de la lectura real del repo `Spark_S3_Financial_ETL` y del
plan `plan_ejecucion_aws.md`. Las IPs ya están tomadas de tu propio
`architecture/ec2_ip.md`, así que los comandos de abajo ya están listos
para copiar y pegar, sin placeholders.

```
ec2_rabbit_ip:  10.0.2.198
ec2_master_ip:  10.0.2.241
ec2_worker1_ip: 10.0.2.120
ec2_worker2_ip: 10.0.2.8
ec2_worker3_ip: 10.0.2.123
ec2_proxy_ip:   3.134.55.16  (pública)
bucket real:    orion-financial-crisis-data
```

---

## 0. Lo que ya corregí en el código (no necesitas hacer nada aquí)

| Archivo | Cambio |
|---|---|
| `architecture/pipelines/dags/common/financial_landing.py` | El valor *default* de `BUCKET` (solo se usa si `FINANCIAL_S3_BUCKET` no está seteada) apuntaba al nombre largo viejo (`...395840094505-us-east-2-an`). Lo alineé a `orion-financial-crisis-data`. |
| `architecture/pipelines/spark_jobs/test_s3_spark.py` | Mismo caso: el path de prueba por defecto. Alineado. |

**Importante:** ninguno de los dos era un bug que rompiera el pipeline real,
porque tu `.env` ya define `FINANCIAL_S3_BUCKET=orion-financial-crisis-data`
de forma consistente en las 5 variables, y eso pisa el default. Lo corregí
para que no quede un nombre de bucket inexistente "fantasma" en el repo.

**Dato útil:** el DAG `financial_crisis_kaggle_to_raw.py` **ya tiene
encadenadas** las 4 fases Spark (landing→raw, raw→staging,
staging→intermediate, intermediate→mart). La sección 8.5 del plan que
recibiste, que sugiere "agregar esas tareas al DAG", está desactualizada —
ya están implementadas y conectadas con `>>`.

---

## 1. Lo que solo tú puedes ejecutar (yo no tengo acceso a AWS ni SSH)

No tengo credenciales de AWS ni acceso a tus 6 EC2 desde este entorno, así
que toda la infraestructura (S3, IAM, VPC, Security Groups, y los `docker
compose up` en cada máquina) la tienes que correr tú. Abajo está cada paso
con los comandos exactos.

### 1.1 S3 + IAM (FASE 1 del plan)

```bash
# Bucket — usa el nombre que YA está en tu .env, no el nombre largo del plan
aws s3api create-bucket \
  --bucket orion-financial-crisis-data \
  --region us-east-2 \
  --create-bucket-configuration LocationConstraint=us-east-2

aws s3api put-public-access-block \
  --bucket orion-financial-crisis-data \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning \
  --bucket orion-financial-crisis-data \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket orion-financial-crisis-data \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'

for p in landing raw staging intermediate mart logs manifests; do
  aws s3api put-object --bucket orion-financial-crisis-data --key "dev/financial_crisis/${p}/"
done
```

```bash
# IAM policy — usa el ARN del bucket real
cat > orion-s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket","s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::orion-financial-crisis-data"
    },
    {
      "Sid": "ReadWriteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject","s3:PutObject","s3:DeleteObject",
        "s3:AbortMultipartUpload","s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::orion-financial-crisis-data/*"
    }
  ]
}
EOF

aws iam create-policy \
  --policy-name orion-data-platform-s3-policy \
  --policy-document file://orion-s3-policy.json

aws iam create-role \
  --role-name orion-data-platform-s3-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy \
  --role-name orion-data-platform-s3-role \
  --policy-arn arn:aws:iam::<TU_ACCOUNT_ID>:policy/orion-data-platform-s3-policy

aws iam create-instance-profile --instance-profile-name orion-data-platform-s3-profile
aws iam add-role-to-instance-profile \
  --instance-profile-name orion-data-platform-s3-profile \
  --role-name orion-data-platform-s3-role
```

Asocia el instance profile a las 6 EC2 (master + 3 workers + rabbitmq + proxy)
**antes** de levantar cualquier contenedor:

```bash
aws ec2 associate-iam-instance-profile \
  --instance-id <ID_EC2> \
  --iam-instance-profile Name=orion-data-platform-s3-profile
```

Verificación rápida (se puede correr desde cualquier EC2 con el rol ya
asociado, sin necesidad de contenedores):

```bash
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/
```

### 1.2 VPC, subredes, SGs

Esto el plan ya lo documenta bien tal cual (sección 1.3 y 1.4). Un detalle
que confirmé en el código y que vale la pena resaltar: en `sg-master`, el
puerto **7077** (Spark master RPC) y **8082** (Spark UI) deben aceptar
tráfico de `sg-workers` *y* de `sg-master` mismo — así está en el plan,
está bien. No encontré inconsistencias entre las reglas de SG del plan y
los puertos que el código realmente usa (`7077`, `7079`, `7080`, `4040`,
`8081`, `8082`, `8080`, `5555`).

### 1.3 EC2 + Docker (FASE 2)

Igual que el plan, sin cambios. Un paso que el plan **no incluye** y que
sí es necesario, porque los compose de Spark referencian una red externa:

```bash
# EJECUTAR EN TODAS LAS EC2 (master + 3 workers) ANTES de levantar Spark
docker network create spark-net
```

Si no creas esta red antes, `docker compose -f
spark_orion/master/docker-compose.master.yml up -d` o el equivalente de
worker fallarán con `network spark-net declared as external, but could
not be found`.

---

## 2. Despliegue por EC2 — comandos exactos con tus IPs

### rabbitmq-server (IP privada 10.0.2.198)

```bash
cd ~/Spark_S3_Financial_ETL/architecture/rabbitmq
docker compose up -d
docker ps
curl -u airflow:airflow http://localhost:15672/api/overview
```

### airflow-master + Spark Master (IP privada 10.0.2.241)

Confirma que tu `.env` en esta máquina tenga `SPARK_DRIVER_HOST=` (vacío,
porque el master no corre tareas Celery), y que `SPARK_MASTER_CONNECT_HOST`
no se use aquí (solo aplica en los workers).

```bash
cd ~/Spark_S3_Financial_ETL/architecture
docker network create spark-net   # si no existe aún

docker compose -f master/docker-compose.yml run --rm airflow-init
docker compose -f master/docker-compose.yml up -d

docker compose --env-file .env \
  -f spark_orion/master/docker-compose.master.yml up -d --build
```

### airflow-worker-1 (IP privada 10.0.2.120)

```bash
cd ~/Spark_S3_Financial_ETL/architecture
docker network create spark-net   # si no existe aún

# Confirma en .env de ESTA máquina:
#   CELERY_HOSTNAME=worker1
#   SPARK_DRIVER_HOST=10.0.2.120
#   WORKER_ID=1
#   SPARK_MASTER_CONNECT_HOST=10.0.2.241
grep -E "CELERY_HOSTNAME|SPARK_DRIVER_HOST|WORKER_ID|SPARK_MASTER_CONNECT_HOST" .env

docker compose -f worker/docker-compose.yml up -d --build
docker compose --env-file .env \
  -f spark_orion/worker/docker-compose.worker.yml up -d --build
```

### airflow-worker-2 (IP privada 10.0.2.8)

Mismo bloque que worker-1, cambiando en el `.env` de esta máquina:

```env
CELERY_HOSTNAME=worker2
SPARK_DRIVER_HOST=10.0.2.8
WORKER_ID=2
SPARK_MASTER_CONNECT_HOST=10.0.2.241
```

### airflow-worker-3 (IP privada 10.0.2.123)

```env
CELERY_HOSTNAME=worker3
SPARK_DRIVER_HOST=10.0.2.123
WORKER_ID=3
SPARK_MASTER_CONNECT_HOST=10.0.2.241
```

### proxy-server (IP pública 3.134.55.16)

```bash
cd ~/Spark_S3_Financial_ETL/architecture/nginx-proxy-manager
docker compose up -d
# UI en http://3.134.55.16:81  → admin@example.com / changeme (cambiar de inmediato)
```

Proxy Hosts a configurar (Forward Hostname/IP = las privadas de arriba):

| Dominio | Forward a | Puerto |
|---|---|---|
| orion-airflow.coderhivex.com | 10.0.2.241 | 8080 |
| orion-flower.coderhivex.com | 10.0.2.241 | 5555 |
| orion-spark.coderhivex.com | 10.0.2.241 | 8082 |
| orion-rabbit.coderhivex.com | 10.0.2.198 | 15672 |

---

## 3. Airflow Connections y Variables (FASE 5)

Sin cambios respecto al plan. Resumen rápido:

```
Conn ID: aws_orion_s3      | Type: Amazon Web Services | Extra: {"region_name": "us-east-2"}
Conn ID: spark_standalone  | Type: Apache Spark | Host: spark://spark-master | Port: 7077
Variable: KAGGLE_API_TOKEN = <el del .env>
```

```bash
docker exec -it airflow-scheduler airflow pools list
# Debe aparecer: spark_jobs_pool (4 slots)
```

**Nota sobre el pool (no encontré esto documentado en el plan):** revisé
`SPARK_TOTAL_EXECUTOR_CORES=4` y `SPARK_EXECUTOR_CORES=2` en tu `.env`.
Con 3 workers de 2 cores cada uno (6 cores totales) y el pool fijo en 4
slots, si llegaran a correr 2 jobs Spark al mismo tiempo (cada uno pidiendo
hasta 4 cores), competirían por núcleos ya repartidos. Si vas a activar las
fases staging/intermediate/mart en producción con concurrencia real, te
recomiendo bajar el pool a 3 slots:

```bash
docker exec -it airflow-scheduler airflow pools set spark_jobs_pool 3 "Pool for Spark Submit jobs"
```

---

## 4. Verificación y ejecución del DAG (FASE 6 y 7)

Igual que el plan original, sin cambios:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
curl http://169.254.169.254/latest/meta-data/iam/security-credentials/

docker exec -it airflow-worker python3 -c "
import boto3
s3 = boto3.client('s3', region_name='us-east-2')
print(s3.list_buckets())
"
```

Trigger del DAG (ya incluye las 4 fases encadenadas, no necesitas tocar
nada del DAG):

```json
{
  "ingestion_date": "2026-06-24",
  "landing_run_id": "airflow_20260624T120000"
}
```

Flujo real de tareas (confirmado leyendo el código, más completo que lo
que indicaba el plan):

```
build_landing_context
  → configure_kaggle_credentials
  → download_unzip_upload_sources_to_landing
  → generate_landing_manifest
  → validate_landing_files
  → spark_landing_to_raw
  → generate_raw_manifest
  → spark_raw_to_staging
  → spark_staging_to_intermediate
  → spark_intermediate_to_mart
```

---

## 5. Riesgos de seguridad confirmados en el código (no son AWS, son del repo)

- `architecture/.env` contiene en texto plano la contraseña de PostgreSQL
  apuntando a una **IP pública** (`15.204.173.204:6432`). Si ya tienes el
  Security Group de esa base de datos restringido a las IPs privadas de
  tus EC2 (o, mejor, planeas migrar a RDS dentro de la VPC), perfecto; si
  no, es el punto de mayor riesgo de todo el despliegue.
- `KAGGLE_API_TOKEN` también en texto plano en el mismo `.env`. Para
  producción, AWS Secrets Manager o Parameter Store sería lo correcto;
  como mínimo confirma que `.env` sigue en `.gitignore` (ya lo está, según
  el repo).

---

## 6. Checklist de orden final

```
[ ] 1. Bucket S3 + IAM Role (sección 1.1 de esta guía)
[ ] 2. VPC + Subnets + IGW + NAT + Route Tables + Security Groups
[ ] 3. EC2: Docker instalado + repo clonado + IAM Role asociado
[ ] 4. .env editado por máquina (CELERY_HOSTNAME, SPARK_DRIVER_HOST, WORKER_ID)
[ ] 5. docker network create spark-net  (en master y en cada worker)
[ ] 6. RabbitMQ up
[ ] 7. Airflow Master: init → up
[ ] 8. Spark Master up
[ ] 9. Airflow Worker + Spark Worker up (en cada una de las 3 EC2 worker)
[ ] 10. Nginx Proxy Manager up + 4 Proxy Hosts configurados
[ ] 11. Connections (aws_orion_s3, spark_standalone) + Variable KAGGLE_API_TOKEN
[ ] 12. Verificar Flower, Spark UI, credenciales IAM, acceso boto3 a S3
[ ] 13. Trigger financial_crisis_kaggle_to_raw
```
