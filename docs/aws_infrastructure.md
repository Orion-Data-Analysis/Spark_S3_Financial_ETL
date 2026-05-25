# Documentacion de Infraestructura AWS

Esta guia detalla el paso a paso para construir la arquitectura de red y desplegar las 6 instancias necesarias para el proyecto **Data Platform (Airflow + RabbitMQ + Nginx Proxy + Spark Standalone)** usando la consola web de AWS.

La arquitectura final queda asi:

| EC2 | Servicios principales |
| :--- | :--- |
| `proxy-server` | Nginx Proxy Manager |
| `rabbitmq-server` | RabbitMQ |
| `airflow-master` | Airflow API Server, Scheduler, Triggerer, Flower y Spark Master |
| `airflow-worker-1` | Airflow Worker y Spark Worker |
| `airflow-worker-2` | Airflow Worker y Spark Worker |
| `airflow-worker-3` | Airflow Worker y Spark Worker |

---

## 1. Diseno de Red

Para aislar el trafico interno de internet, se crea una VPC con una subred publica y una subred privada.

### Paso 1: Crear la VPC

1. Ve al servicio **VPC** en la consola de AWS.
2. Haz clic en **Create VPC**.
3. Selecciona **VPC only**.
4. **Name tag:** `data-platform-vpc`.
5. **IPv4 CIDR block:** `10.0.0.0/16`.
6. Haz clic en **Create VPC**.

### Paso 2: Crear la subred publica

Esta subred es para `proxy-server`.

1. Ve a **VPC > Subnets > Create subnet**.
2. Selecciona la VPC `data-platform-vpc`.
3. **Subnet name:** `data-platform-public-subnet`.
4. **Availability Zone:** usa una zona, por ejemplo `us-east-2a`.
5. **IPv4 CIDR block:** `10.0.1.0/24`.
6. Crea la subred.
7. Selecciona la subred creada, entra a **Actions > Edit subnet settings** y activa **Enable auto-assign public IPv4 address**.

### Paso 3: Crear la subred privada

Esta subred es para `airflow-master`, `airflow-workers`, `rabbitmq-server` y Spark.

1. Haz clic en **Create subnet**.
2. Selecciona la VPC `data-platform-vpc`.
3. **Subnet name:** `data-platform-private-subnet`.
4. **Availability Zone:** usa la misma zona anterior.
5. **IPv4 CIDR block:** `10.0.2.0/24`.
6. Crea la subred.
7. No actives IP publica automatica para esta subred.

---

## 2. Internet Gateway, NAT Gateway y Rutas

Las maquinas privadas necesitan salida a internet para descargar paquetes e imagenes Docker, pero no deben recibir trafico publico directo. Para eso se usa un NAT Gateway.

### Paso 1: Crear Internet Gateway

1. Ve a **VPC > Internet Gateways > Create internet gateway**.
2. **Name tag:** `data-platform-igw`.
3. Crea el IGW.
4. Seleccionalo y entra a **Actions > Attach to VPC**.
5. Asociarlo a `data-platform-vpc`.

### Paso 2: Crear NAT Gateway

1. Ve a **VPC > NAT Gateways > Create NAT gateway**.
2. **Name:** `data-platform-nat`.
3. **Subnet:** `data-platform-public-subnet`.
4. **Elastic IP allocation ID:** asigna una Elastic IP.
5. Crea el NAT Gateway y espera a que quede en estado `Available`.

### Paso 3: Configurar tablas de rutas

**Tabla publica:**

1. Ve a **Route Tables**.
2. Renombra la tabla publica como `data-platform-public-rt`.
3. En **Routes**, agrega:
   - **Destination:** `0.0.0.0/0`
   - **Target:** `data-platform-igw`
4. En **Subnet associations**, asocia `data-platform-public-subnet`.

**Tabla privada:**

1. Crea una tabla llamada `data-platform-private-rt`.
2. Asociarla a `data-platform-vpc`.
3. En **Routes**, agrega:
   - **Destination:** `0.0.0.0/0`
   - **Target:** `data-platform-nat`
4. En **Subnet associations**, asocia `data-platform-private-subnet`.

---

## 3. Grupos de Seguridad

Crea los grupos en **EC2 > Security Groups > Create security group** dentro de la VPC `data-platform-vpc`.

### 3.1 `SG_PROXY`

Asignar a la EC2 `proxy-server`.

| Tipo | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| HTTP | `80` | `0.0.0.0/0` | Trafico web y redirecciones |
| HTTPS | `443` | `0.0.0.0/0` | Trafico web seguro |
| Custom TCP | `81` | Tu IP publica | Panel admin de Nginx Proxy Manager |

### 3.2 `SG_MASTER`

Asignar a la EC2 `airflow-master`, donde tambien se levantara `spark-master`.

| Tipo | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| SSH | `22` | Tu IP publica, VPN o Bastion | Administracion |
| Custom TCP | `8080` | `SG_PROXY` | Airflow UI |
| Custom TCP | `5555` | `SG_PROXY` | Flower UI |
| Custom TCP | `8082` | `SG_PROXY` | Spark Master UI |
| Custom TCP | `8793` | `SG_WORKERS` | Logs de tareas Airflow |
| Custom TCP | `7077` | `SG_WORKERS` | Spark Workers se registran en Spark Master |
| Custom TCP | `7079` | `SG_WORKERS` | Puerto fijo recomendado para Spark Driver |
| Custom TCP | `7080` | `SG_WORKERS` | Puerto fijo recomendado para Spark Block Manager |
| Custom TCP | `4040` | `SG_PROXY` o `SG_WORKERS` | Spark Application UI, opcional |

### 3.3 `SG_RABBITMQ`

Asignar a la EC2 `rabbitmq-server`.

| Tipo | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| SSH | `22` | Tu IP publica, VPN o Bastion | Administracion |
| Custom TCP | `5672` | `SG_MASTER`, `SG_WORKERS` | Cola AMQP de Celery |
| Custom TCP | `15672` | `SG_PROXY` | Panel web RabbitMQ |
| Custom TCP | `4369` | `SG_RABBITMQ` | EPMD, solo si haces cluster RabbitMQ |

### 3.4 `SG_WORKERS`

Asignar a las EC2 `airflow-worker-1`, `airflow-worker-2` y `airflow-worker-3`, donde tambien se levantaran los Spark Workers.

| Tipo | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| SSH | `22` | Tu IP publica, VPN o Bastion | Administracion |
| Custom TCP | `8793` | `SG_MASTER` | Airflow Master consulta logs del worker |
| Custom TCP | `8081` | `SG_PROXY` | Spark Worker UI |
| All TCP | `0-65535` | `SG_MASTER` | Comunicacion Spark Master hacia workers |
| All TCP | `0-65535` | `SG_WORKERS` | Comunicacion entre Spark Workers y retorno al driver |

### 3.5 Reglas Outbound

Si los Security Groups conservan la regla default de AWS:

| Tipo | Protocolo | Puerto | Destino |
| :--- | :--- | :--- | :--- |
| All traffic | All | All | `0.0.0.0/0` |

no necesitas agregar outbound extra para esta etapa.

Si tu outbound esta restringido, agrega como minimo:

| Security Group | Salida hacia | Puerto | Proposito |
| :--- | :--- | :--- | :--- |
| `SG_MASTER` | `SG_WORKERS` | `7079`, `7080` | Spark Driver / Block Manager |
| `SG_WORKERS` | `SG_MASTER` | `7077` | Conexion al Spark Master |
| `SG_WORKERS` | `SG_RABBITMQ` | `5672` | Consumir tareas Celery |
| `SG_MASTER`, `SG_WORKERS` | Internet via NAT | All | Docker, apt, pip, S3 |

### 3.6 Resumen de Reglas Inbound

| Grupo | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| `SG_PROXY` | `80`, `443` | `0.0.0.0/0` | Acceso web publico |
| `SG_PROXY` | `81` | Tu IP | Admin Nginx Proxy Manager |
| `SG_MASTER` | `8080` | `SG_PROXY` | Airflow UI |
| `SG_MASTER` | `5555` | `SG_PROXY` | Flower UI |
| `SG_MASTER` | `8082` | `SG_PROXY` | Spark Master UI |
| `SG_MASTER` | `7077` | `SG_WORKERS` | Registro Spark Workers |
| `SG_MASTER` | `8793` | `SG_WORKERS` | Logs Airflow |
| `SG_MASTER` | `7079`, `7080` | `SG_WORKERS` | Spark Driver / Block Manager |
| `SG_RABBITMQ` | `5672` | `SG_MASTER`, `SG_WORKERS` | Mensajeria Celery |
| `SG_RABBITMQ` | `15672` | `SG_PROXY` | Panel RabbitMQ |
| `SG_WORKERS` | `8793` | `SG_MASTER` | Logs Airflow |
| `SG_WORKERS` | `8081` | `SG_PROXY` | Spark Worker UI |
| `SG_WORKERS` | All TCP | `SG_MASTER`, `SG_WORKERS` | Comunicacion interna Spark |

No olvides permitir en tu base de datos PostgreSQL externa el puerto `5432` desde las IP privadas de `airflow-master` y los `airflow-workers`.

---

## 4. Despliegue de Instancias EC2

Ve a **EC2 > Instances > Launch instances**.

### Instancia 1: Nginx Proxy Manager

| Campo | Valor |
| :--- | :--- |
| Name | `proxy-server` |
| OS | Ubuntu Server 24.04 LTS |
| Instance type | `t3.small` o `t3.micro` |
| Subnet | `data-platform-public-subnet` |
| Auto-assign public IP | Enable |
| Security Group | `SG_PROXY` |

### Instancia 2: RabbitMQ

| Campo | Valor |
| :--- | :--- |
| Name | `rabbitmq-server` |
| OS | Ubuntu Server 24.04 LTS |
| Instance type | `t3.medium` |
| Subnet | `data-platform-private-subnet` |
| Auto-assign public IP | Disable |
| Security Group | `SG_RABBITMQ` |

### Instancia 3: Airflow Master + Spark Master

| Campo | Valor |
| :--- | :--- |
| Name | `airflow-master` |
| OS | Ubuntu Server 24.04 LTS |
| Instance type | `t3.large` recomendado, `t3.medium` para pruebas |
| Subnet | `data-platform-private-subnet` |
| Auto-assign public IP | Disable |
| Security Group | `SG_MASTER` |

### Instancias 4, 5 y 6: Airflow Workers + Spark Workers

| Campo | Valor |
| :--- | :--- |
| Name | `airflow-worker` |
| Number of instances | `3` |
| OS | Ubuntu Server 24.04 LTS |
| Instance type | `t3.medium` o superior |
| Subnet | `data-platform-private-subnet` |
| Auto-assign public IP | Disable |
| Security Group | `SG_WORKERS` |

---

## 5. Configuracion de Spark Standalone

Spark se despliega usando los compose del proyecto:

```text
spark_orion/master/docker-compose.master.yml
spark_orion/worker/docker-compose.worker.yml
```

La imagen usada es la oficial de Apache:

```env
SPARK_IMAGE=apache/spark:3.5.0
```

### 5.1 Variables Spark en el `.env`

El archivo `.env` principal debe estar en la raiz de `Done-data-platform`.

```env
SPARK_IMAGE=apache/spark:3.5.0
SPARK_NETWORK=spark-net

SPARK_MASTER_BIND_HOST=spark-master
SPARK_MASTER_CONNECT_HOST=<IP_PRIVADA_DE_AIRFLOW_MASTER>
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8082

WORKER_ID=1
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_WEBUI_PORT=8081
SPARK_WORKER_HOST=0.0.0.0

SPARK_LOGS_PATH=../runtime/logs
SPARK_WORK_PATH=../runtime/work

RESTART_POLICY=unless-stopped
```

`SPARK_MASTER_CONNECT_HOST` debe ser la IP privada de la EC2 `airflow-master`. Ejemplo:

```env
SPARK_MASTER_CONNECT_HOST=10.0.2.251
```

No uses la IP publica para este valor.

Si los Spark Workers estan en EC2 diferentes, todos pueden usar:

```env
SPARK_WORKER_WEBUI_PORT=8081
```

Por orden, cambia `WORKER_ID` en cada EC2:

| EC2 | `WORKER_ID` | `SPARK_WORKER_WEBUI_PORT` |
| :--- | :--- | :--- |
| `airflow-worker-1` | `1` | `8081` |
| `airflow-worker-2` | `2` | `8081` |
| `airflow-worker-3` | `3` | `8081` |

### 5.2 Levantar Spark Master

En la EC2 `airflow-master`:

```bash
cd ~/Done-data-platform

docker network create spark-net 2>/dev/null || true
mkdir -p spark_orion/runtime/logs spark_orion/runtime/work

docker compose \
  --env-file .env \
  -f spark_orion/master/docker-compose.master.yml \
  up -d --force-recreate
```

Validar:

```bash
docker ps
docker logs spark-master --tail 100
```

La salida esperada debe incluir:

```text
Successfully started service 'sparkMaster' on port 7077
MasterWebUI ... started at http://spark-master:8082
New state: ALIVE
```

### 5.3 Levantar Spark Worker

En cada EC2 `airflow-worker`:

```bash
cd ~/Done-data-platform

docker network create spark-net 2>/dev/null || true
mkdir -p spark_orion/runtime/logs spark_orion/runtime/work

docker compose \
  --env-file .env \
  -f spark_orion/worker/docker-compose.worker.yml \
  up -d --force-recreate
```

Validar:

```bash
docker ps
docker logs spark-worker-1 --tail 100
```

El worker debe conectarse a:

```text
spark://<IP_PRIVADA_DE_AIRFLOW_MASTER>:7077
```

En la UI del Spark Master (`8082`) deben aparecer los workers registrados.

---

## 6. Conexion de Airflow hacia Spark

En Airflow crea o valida la conexion `spark_standalone`.

| Campo | Valor |
| :--- | :--- |
| Connection Id | `spark_standalone` |
| Connection Type | `Spark` |
| Host | `spark://<IP_PRIVADA_DE_AIRFLOW_MASTER>` |
| Port | `7077` |

Ejemplo:

```text
spark://10.0.2.251:7077
```

Como Airflow usa `CeleryExecutor`, los DAGs normalmente corren en los Airflow Workers. Por eso se recomienda permitir comunicacion interna entre `SG_WORKERS` y desde `SG_MASTER` hacia `SG_WORKERS`.

Para evitar puertos aleatorios en jobs Spark, configura en tus Spark jobs:

```python
.config("spark.driver.port", "7079")
.config("spark.blockManager.port", "7080")
.config("spark.ui.port", "4040")
```

---

## 7. Nginx Proxy Manager

Configura dominios o subdominios apuntando a IPs privadas:

| Dominio sugerido | Forward Hostname / IP | Forward Port |
| :--- | :--- | :--- |
| `orion-airflow.coderhivex.com` | IP privada de `airflow-master` | `8080` |
| `orion-flower.coderhivex.com` | IP privada de `airflow-master` | `5555` |
| `orion-spark-master.coderhivex.com` | IP privada de `airflow-master` | `8082` |
| `orion-rabbitmq.coderhivex.com` | IP privada de `rabbitmq-server` | `15672` |
| `orion-spark-worker-1.coderhivex.com` | IP privada de `airflow-worker-1` | `8081` |

Activa **Websockets Support** en los proxy hosts donde aplique.

---

## 8. Checklist Final

1. `SG_MASTER` permite `7077` desde `SG_WORKERS`.
2. `SG_MASTER` permite `8082` desde `SG_PROXY`.
3. `SG_WORKERS` permite `8081` desde `SG_PROXY`.
4. `SG_WORKERS` permite All TCP desde `SG_MASTER` y `SG_WORKERS`.
5. El `.env` usa `SPARK_IMAGE=apache/spark:3.5.0`.
6. `SPARK_MASTER_CONNECT_HOST` es la IP privada de `airflow-master`.
7. Existe la red Docker en cada EC2:

```bash
docker network ls | grep spark-net
```

8. Spark Master esta vivo:

```bash
docker logs spark-master --tail 100
```

9. Cada Spark Worker esta registrado en la UI del Spark Master.

---

## 9. Notas Finales

1. Guarda las IP privadas de `airflow-master`, `rabbitmq-server` y cada `airflow-worker`.
2. En el `.env` de Airflow, usa la IP privada de `rabbitmq-server` para Celery.
3. En el `.env` de Spark, usa la IP privada de `airflow-master` en `SPARK_MASTER_CONNECT_HOST`.
4. No uses `--remove-orphans` cuando levantes Spark si Airflow ya esta corriendo en la misma carpeta/proyecto, porque podrias bajar contenedores de Airflow.
5. Si el outbound de los security groups esta abierto con `All traffic -> 0.0.0.0/0`, no necesitas reglas outbound extra para esta etapa.
