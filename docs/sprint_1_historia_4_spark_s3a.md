# Sprint 1 - Historia 4: Conector Spark-S3 con s3a

Esta guia documenta la implementacion de la Historia de Usuario 4: habilitar Apache Spark para leer y escribir en S3 usando el protocolo `s3a://`.

## Alcance implementado

- Se agrego una imagen Spark propia en `architecture/spark_orion/Dockerfile`.
- La imagen extiende `apache/spark:3.5.0`.
- Se instalan los JARs compatibles con Hadoop 3.3.4:
  - `hadoop-aws-3.3.4.jar`
  - `aws-java-sdk-bundle-1.12.262.jar`
- Los JARs quedan en `/opt/spark/jars/`.
- Se crea el alias `/opt/bitnami/spark` hacia `/opt/spark` para cubrir la ruta estandar indicada en la historia.
- Los compose de Spark Master y Worker ahora construyen la imagen local si no existe.
- La variable `SPARK_IMAGE` queda apuntando a `orion/spark-s3a:3.5.0`.
- Se agrego el helper reutilizable `architecture/pipelines/spark_jobs/session.py`.

## Construccion de la imagen

Ejecutar desde `architecture/spark_orion`:

```bash
docker build -t orion/spark-s3a:3.5.0 .
```

Tambien puede construirse automaticamente al levantar los compose:

```bash
docker compose \
  --env-file ../.env \
  -f master/docker-compose.master.yml \
  up -d --build
```

En cada worker:

```bash
docker compose \
  --env-file ../.env \
  -f worker/docker-compose.worker.yml \
  up -d --build
```

## Validacion de JARs

En el Master o en un Worker:

```bash
docker exec -it spark-master ls -l /opt/spark/jars/hadoop-aws-3.3.4.jar
docker exec -it spark-master ls -l /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
docker exec -it spark-master ls -l /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar
```

Los tres comandos deben mostrar archivos legibles.

## SparkSession base

Los jobs PySpark deben reutilizar:

```python
from spark_jobs.session import build_spark_session

spark = build_spark_session()
```

La sesion incluye:

```python
.config("spark.sql.session.timeZone", "UTC")
.config("spark.sql.shuffle.partitions", "8")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
.config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
```

## Validacion esperada

Un job que escriba o lea `s3a://...` no debe fallar por:

- `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`
- ausencia de `hadoop-aws`
- ausencia de `aws-java-sdk-bundle`
- credenciales explicitas faltantes, siempre que la EC2 tenga asociado el rol IAM correcto

## Smoke test replicable

Se agrego el script:

```text
architecture/pipelines/spark_jobs/test_s3_spark.py
```

### Prueba local de JARs y clase S3A

Esta prueba valida que Spark puede cargar la clase `S3AFileSystem`. No requiere credenciales AWS.

Desde la raiz del proyecto:

```powershell
docker build -t orion/spark-s3a:3.5.0 architecture/spark_orion
docker run --rm `
  -v ${PWD}\architecture\pipelines:/opt/pipelines `
  -e PYTHONPATH=/opt/pipelines `
  orion/spark-s3a:3.5.0 `
  /opt/spark/bin/spark-submit /opt/pipelines/spark_jobs/test_s3_spark.py
```

En Linux/EC2:

```bash
docker build -t orion/spark-s3a:3.5.0 architecture/spark_orion
docker run --rm \
  -v "$PWD/architecture/pipelines:/opt/pipelines" \
  -e PYTHONPATH=/opt/pipelines \
  orion/spark-s3a:3.5.0 \
  /opt/spark/bin/spark-submit /opt/pipelines/spark_jobs/test_s3_spark.py
```

Salida esperada:

```text
OK: Spark cargo la clase org.apache.hadoop.fs.s3a.S3AFileSystem
OK: validacion local completada. Use --s3-e2e en la EC2 para probar S3.
```

### Prueba end-to-end en EC2 contra S3

Ejecutar esta prueba solo en una EC2 que tenga asociado el IAM Role del proyecto con permisos sobre el bucket.

```powershell
docker run --rm `
  -v ${PWD}\architecture\pipelines:/opt/pipelines `
  -e PYTHONPATH=/opt/pipelines `
  -e FINANCIAL_AWS_REGION=us-east-2 `
  orion/spark-s3a:3.5.0 `
  /opt/spark/bin/spark-submit /opt/pipelines/spark_jobs/test_s3_spark.py --s3-e2e
```

En Linux/EC2:

```bash
docker run --rm \
  -v "$PWD/architecture/pipelines:/opt/pipelines" \
  -e PYTHONPATH=/opt/pipelines \
  -e FINANCIAL_AWS_REGION=us-east-2 \
  orion/spark-s3a:3.5.0 \
  /opt/spark/bin/spark-submit /opt/pipelines/spark_jobs/test_s3_spark.py --s3-e2e
```

Salida esperada:

```text
OK: Spark cargo la clase org.apache.hadoop.fs.s3a.S3AFileSystem
Escribiendo dataset de prueba en s3a://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/test_spark_e2e
Leyendo dataset de prueba desde S3
+----------------+-------+----------+
|source_system   |status |date      |
+----------------+-------+----------+
|digital_accounts|success|2026-05-26|
|virtual_wallets |pending|2026-05-26|
+----------------+-------+----------+
OK: prueba end-to-end Spark S3A exitosa
```

Si falla con `ClassNotFoundException`, el problema esta en la imagen o en los JARs. Si falla con `AccessDenied`, `Unable to load credentials` o errores de red, la configuracion S3A ya cargo y el problema esta en IAM, red o permisos del bucket.

