# Sprint 1 - Historia 3: Conexion de Airflow con S3

Esta guia documenta todo lo realizado para completar la **Historia de Usuario 3: Conexion de la Orquestacion Airflow con el Data Lake S3**.

El objetivo fue que Airflow pudiera conectarse al bucket S3 del proyecto usando el IAM Role de las EC2, sin guardar access keys en Airflow ni en archivos `.env`.

---

## 1. Alcance de la Historia

La historia pide cumplir tres puntos:

| Criterio | Estado |
| :--- | :--- |
| Crear una conexion segura `aws_orion_s3` en Airflow sin contrasenas explicitas | Completado |
| Actualizar `.env` con rutas nativas de S3 | Completado |
| Configurar logs remotos de Airflow hacia S3 | Completado |

La validacion final fue exitosa desde el contenedor `master-airflow-apiserver-1`:

```text
Found credentials from IAM Role: orion-data-platform-s3-role
Bucket exists: True
```

---

## 2. Cambios realizados en el `.env`

Se actualizaron las variables del archivo `.env` principal del proyecto.

Ruta en las EC2:

```text
~/Done-data-platform/.env
```

Ruta en el repositorio:

```text
architecture/.env
```

### 2.1 Variables del Data Lake S3

```env
FINANCIAL_ENV=dev
FINANCIAL_AWS_REGION=us-east-2
FINANCIAL_S3_BUCKET=orion-financial-crisis-data-395840094505-us-east-2-an
FINANCIAL_S3_DOMAIN=financial_crisis
FINANCIAL_S3_BASE=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
FINANCIAL_INPUT_PATH=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing
FINANCIAL_DATA_LAKE_ROOT=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
```

Estas variables permiten que los DAGs y scripts no dependan de rutas locales como `/opt/data/`.

### 2.2 Conexion AWS para Airflow

Se agrego la conexion `aws_orion_s3` por variable de entorno:

```env
AIRFLOW_CONN_AWS_ORION_S3={"conn_type":"aws","extra":{"region_name":"us-east-2"}}
```

Esta conexion no contiene:

- `AWS Access Key ID`
- `AWS Secret Access Key`
- tokens manuales

Airflow usa las credenciales temporales del IAM Role asociado a la EC2.

Airflow permite crear conexiones por variables `AIRFLOW_CONN_{CONN_ID}`. Referencia oficial:

```text
https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
```

### 2.3 Logs remotos de Airflow en S3

Se cambio la configuracion de logging:

```env
AIRFLOW__LOGGING__REMOTE_LOGGING=true
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/airflow
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_orion_s3
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=false
```

Con esto, los logs de tareas deben escribirse en:

```text
s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/airflow/
```

---

## 3. Ajuste de PostgreSQL usado por Airflow

Durante la implementacion, Airflow no levantaba porque apuntaba a una IP anterior de PostgreSQL:

```text
51.222.142.204:5432
```

El PostgreSQL real estaba corriendo en la EC2 `proxy-server`, con IP privada:

```text
23.0.1.199
```

Se reemplazaron estas variables:

```env
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://orion:Orion123%21@23.0.1.199:5432/orion_db
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql+psycopg2://orion:Orion123%21@23.0.1.199:5432/orion_db
```

Nota: la contrasena real es:

```text
Orion123!
```

En la URL se escribio como:

```text
Orion123%21
```

porque el caracter `!` debe ir codificado como `%21`.

### 3.1 Como se verificaron las credenciales de Postgres

En la EC2 del proxy:

```bash
docker ps
```

Se encontro el contenedor:

```text
postgres-local
```

Luego se inspeccionaron las variables del contenedor:

```bash
docker inspect postgres-local | grep -i "POSTGRES_USER\|POSTGRES_PASSWORD\|POSTGRES_DB"
```

Resultado:

```text
POSTGRES_DB=orion_db
POSTGRES_USER=orion
POSTGRES_PASSWORD=Orion123!
```

### 3.2 Validar conectividad a Postgres desde Airflow Master

En la EC2 `airflow-master`:

```bash
timeout 5 bash -c '</dev/tcp/23.0.1.199/5432' && echo "postgres ok" || echo "postgres bloqueado"
```

Resultado esperado:

```text
postgres ok
```

---

## 4. Cambios necesarios en AWS

### 4.1 Asignar IAM Role a las EC2

El rol creado en la Historia 2 debe estar asociado a:

- `airflow-master`
- `airflow-worker-1`
- `airflow-worker-2`
- `airflow-worker-3`

Rol:

```text
orion-data-platform-s3-role
```

Ruta en AWS:

```text
EC2 > Instances > seleccionar instancia > Actions > Security > Modify IAM role
```

Seleccionar:

```text
orion-data-platform-s3-role
```

Guardar con **Update IAM role**.

### 4.2 Validar el rol desde cada EC2

Primero instalar AWS CLI si no existe:

```bash
sudo apt update
sudo apt install -y awscli
```

Luego ejecutar:

```bash
aws sts get-caller-identity
```

Resultado esperado:

```json
{
  "Account": "395840094505",
  "Arn": "arn:aws:sts::395840094505:assumed-role/orion-data-platform-s3-role/..."
}
```

Si sale:

```text
Unable to locate credentials
```

la EC2 no tiene el IAM Role asignado o no puede acceder al metadata service.

### 4.3 Metadata de EC2 para contenedores Docker

Si el comando funciona en la EC2, pero falla dentro del contenedor con:

```text
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

revisar las opciones de metadata:

```text
EC2 > Instances > seleccionar instancia > Actions > Instance settings > Modify instance metadata options
```

Configuracion recomendada:

```text
IMDS endpoint: Enabled
IMDSv2: Optional o Required
Hop limit: 2
```

El hop limit `2` ayuda a que los contenedores Docker puedan acceder al metadata service de la EC2.

---

## 5. Comandos usados en Airflow Master

### 5.1 Recrear Airflow Master despues de modificar `.env`

En la EC2 `airflow-master`:

```bash
cd ~/Done-data-platform

docker compose \
  --env-file .env \
  -f master/docker-compose.yml \
  up -d --force-recreate
```

Durante la ejecucion puede aparecer:

```text
Found orphan containers ([spark-master])
```

No usar:

```bash
--remove-orphans
```

porque `spark-master` fue levantado con otro compose y podria apagarse accidentalmente.

### 5.2 Verificar contenedores activos

```bash
docker ps
```

Contenedores esperados:

```text
master-airflow-apiserver-1
master-airflow-scheduler-1
master-airflow-dag-processor-1
master-airflow-triggerer-1
master-flower-1
spark-master
```

`master-airflow-init-1` debe quedar en estado `Exited` despues de completar correctamente.

---

## 6. Comandos usados en Airflow Workers

En cada EC2 worker:

```bash
cd ~/Done-data-platform

docker compose \
  --env-file .env \
  -f worker/docker-compose.yml \
  up -d --force-recreate
```

Si tambien se levanta Spark Worker en esa EC2:

```bash
docker network create spark-net 2>/dev/null || true

docker compose \
  --env-file .env \
  -f spark_orion/worker/docker-compose.worker.yml \
  up -d --force-recreate
```

Validar contenedores:

```bash
docker ps
```

Ver logs del Airflow Worker:

```bash
docker logs worker-airflow-worker-1 --tail 100
```

Ver logs del Spark Worker:

```bash
docker logs spark-worker-1 --tail 100
```

Si el nombre cambia, usar `docker ps` para identificar el nombre real.

---

## 7. Validaciones realizadas

### 7.1 Validacion del IAM Role en la EC2

En `airflow-master`:

```bash
aws sts get-caller-identity
```

Resultado obtenido:

```json
{
  "UserId": "AROAVYKPIQEU7R7QLT3XS:i-0ae59ffe9f0674f01",
  "Account": "395840094505",
  "Arn": "arn:aws:sts::395840094505:assumed-role/orion-data-platform-s3-role/i-0ae59ffe9f0674f01"
}
```

### 7.2 Validacion de credenciales dentro del contenedor

Entrar al contenedor:

```bash
docker exec -it master-airflow-apiserver-1 bash
```

Ejecutar:

```bash
python - <<'PY'
import boto3

sts = boto3.client("sts", region_name="us-east-2")
print(sts.get_caller_identity())
PY
```

Resultado esperado:

```text
arn:aws:sts::395840094505:assumed-role/orion-data-platform-s3-role/...
```

### 7.3 Validacion de conexion S3 con `S3Hook`

Dentro del contenedor `master-airflow-apiserver-1`:

```bash
python - <<'PY'
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

bucket = "orion-financial-crisis-data-395840094505-us-east-2-an"
hook = S3Hook(aws_conn_id="aws_orion_s3")
print("Bucket exists:", hook.check_for_bucket(bucket))
PY
```

Resultado obtenido:

```text
Found credentials from IAM Role: orion-data-platform-s3-role
Bucket exists: True
```

El warning:

```text
ProvidersManager.hooks is deprecated
```

no bloquea la ejecucion. Es una advertencia interna del provider de Airflow.

---

## 8. Troubleshooting

### 8.1 `connection timed out` hacia PostgreSQL

Error observado:

```text
connection to server at "51.222.142.204", port 5432 failed: Connection timed out
```

Causa:

- Airflow apuntaba a una IP anterior o no accesible.

Solucion:

- Cambiar el host de PostgreSQL a la IP privada real del proxy:

```text
23.0.1.199
```

- Validar:

```bash
timeout 5 bash -c '</dev/tcp/23.0.1.199/5432' && echo "postgres ok" || echo "postgres bloqueado"
```

### 8.2 `password authentication failed`

Error observado:

```text
FATAL: password authentication failed for user "coder-ra-c6"
```

Causa:

- Usuario y contrasena no correspondian al PostgreSQL real.

Solucion:

- Inspeccionar contenedor `postgres-local`.
- Actualizar `.env` con:

```env
postgresql+psycopg2://orion:Orion123%21@23.0.1.199:5432/orion_db
```

### 8.3 `Unable to locate credentials` en EC2

Error:

```text
Unable to locate credentials. You can configure credentials by running "aws configure".
```

Causa:

- La EC2 no tenia IAM Role asignado.

Solucion:

- Asignar `orion-data-platform-s3-role` desde AWS Console.
- Validar con:

```bash
aws sts get-caller-identity
```

### 8.4 `Unable to locate credentials` dentro del contenedor

Causa:

- El contenedor no podia heredar credenciales desde el metadata service.

Solucion:

- Confirmar que el IAM Role funciona en el host.
- Revisar metadata options.
- Usar hop limit `2` si es necesario.

---

## 9. Checklist para cerrar Historia 3

- [x] `.env` actualizado con variables `FINANCIAL_*`.
- [x] `.env` actualizado con `AIRFLOW_CONN_AWS_ORION_S3`.
- [x] Logs remotos apuntan a S3 con `aws_orion_s3`.
- [x] PostgreSQL apunta a la IP privada correcta del proxy.
- [x] Password de PostgreSQL codificado correctamente en URL.
- [x] IAM Role asociado a `airflow-master`.
- [x] Validacion `aws sts get-caller-identity` exitosa en EC2.
- [x] Validacion `boto3 STS` exitosa dentro del contenedor.
- [x] Validacion `S3Hook.check_for_bucket` exitosa.
- [ ] Repetir validaciones en cada EC2 `airflow-worker`.
- [ ] Ejecutar un DAG real y confirmar logs remotos en S3.

---

## 10. Resultado final

La conexion Airflow -> S3 quedo funcionando mediante IAM Role:

```text
Airflow Container -> EC2 IAM Role -> S3 Data Lake
```

No se usaron access keys estaticas.
