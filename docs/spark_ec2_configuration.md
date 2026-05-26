# Configuracion de EC2 para Spark Master y Spark Workers en AWS

Esta guia explica como deben quedar las instancias EC2, los grupos de seguridad y los archivos `.env` para levantar un cluster **Apache Spark Standalone** usando los `docker-compose` del proyecto.

El objetivo es tener:

- 1 instancia EC2 para **Spark Master**.
- 1 o mas instancias EC2 para **Spark Workers**.
- Comunicacion privada entre Spark Master, Spark Workers y Airflow.
- Acceso controlado a las interfaces web de Spark.

---

## 1. Arquitectura Recomendada

### Componentes

| Componente | Instancia EC2 | Subred | IP publica | Security Group |
| :--- | :--- | :--- | :--- | :--- |
| Spark Master | `spark-master` | Privada | No | `SG_SPARK_MASTER` |
| Spark Worker 1 | `spark-worker-1` | Privada | No | `SG_SPARK_WORKERS` |
| Spark Worker 2 | `spark-worker-2` | Privada | No | `SG_SPARK_WORKERS` |
| Airflow Master | `airflow-master` | Privada | No | `SG_MASTER` |
| Nginx Proxy | `proxy-server` | Publica | Si | `SG_PROXY` |

### Flujo de comunicacion

```text
Airflow Master  ---> Spark Master:7077
Spark Workers   ---> Spark Master:7077
Spark Master    <--> Spark Workers: trafico interno de Spark
Nginx Proxy     ---> Spark Master UI:8082
Nginx Proxy     ---> Spark Worker UI:8081, 8083, 8084...
```

Spark debe comunicarse por IP privada dentro de la VPC. No expongas el puerto `7077` directamente a internet.

---

## 2. Tamano Recomendado de las EC2

### Spark Master

Para el master no se necesita tanta CPU como en los workers, pero si estabilidad.

| Ambiente | Tipo sugerido | Uso |
| :--- | :--- | :--- |
| Desarrollo | `t3.medium` | Pruebas, pocos jobs |
| Proyecto academico estable | `t3.large` | Mejor margen de memoria |
| Produccion basica | `m6i.large` o superior | Cargas mas constantes |

### Spark Workers

Los workers son los que ejecutan el procesamiento pesado.

| Ambiente | Tipo sugerido | Configuracion Spark |
| :--- | :--- | :--- |
| Desarrollo | `t3.medium` | `2 cores`, `2G` |
| Proyecto academico estable | `t3.large` | `2 cores`, `4G` |
| Procesamiento mas pesado | `m6i.large`, `m6i.xlarge` | Segun RAM disponible |

Recomendacion inicial para tu proyecto:

- Spark Master: `t3.medium` o `t3.large`.
- Spark Workers: `t3.medium` si estas probando, `t3.large` si vas a procesar datasets grandes.

---

## 3. Puertos de Spark

| Puerto | Servicio | Donde vive | Quien debe acceder |
| :--- | :--- | :--- | :--- |
| `7077` | Spark Master RPC | Spark Master | Airflow Master y Spark Workers |
| `8082` | Spark Master Web UI | Spark Master | Nginx Proxy o tu IP por VPN/Bastion |
| `8081` | Spark Worker 1 Web UI | Worker 1 | Nginx Proxy o tu IP por VPN/Bastion |
| `8083` | Spark Worker 2 Web UI | Worker 2 | Nginx Proxy o tu IP por VPN/Bastion |
| `8084` | Spark Worker 3 Web UI | Worker 3 | Nginx Proxy o tu IP por VPN/Bastion |
| `4040` | Spark Application UI | Driver | Opcional, para depuracion |

En este proyecto el Spark Master usa `8082` para evitar conflicto con Airflow, que normalmente usa `8080`.

---

## 4. Security Groups

Ve a **EC2 > Security Groups > Create security group** y crea los siguientes grupos dentro de la misma VPC del proyecto.

## 4.1 `SG_SPARK_MASTER`

Asignar este grupo a la EC2 `spark-master`.

### Reglas Inbound

| Tipo | Puerto | Origen | Motivo |
| :--- | :--- | :--- | :--- |
| SSH | `22` | Tu IP publica o Bastion | Administracion por SSH |
| Custom TCP | `7077` | `SG_MASTER` | Airflow envia jobs al Spark Master |
| Custom TCP | `7077` | `SG_SPARK_WORKERS` | Workers se registran en el Master |
| Custom TCP | `8082` | `SG_PROXY` | Publicar Spark Master UI con Nginx Proxy |
| All TCP | `0-65535` | `SG_SPARK_WORKERS` | Comunicacion interna Spark Master/Workers |

### Reglas Outbound

| Tipo | Puerto | Destino | Motivo |
| :--- | :--- | :--- | :--- |
| All traffic | All | `0.0.0.0/0` | Descargar imagenes, paquetes y responder conexiones |

Si quieres endurecer seguridad despues, puedes limitar outbound hacia `SG_SPARK_WORKERS`, S3, NAT Gateway y repositorios necesarios.

---

## 4.2 `SG_SPARK_WORKERS`

Asignar este grupo a todas las EC2 worker: `spark-worker-1`, `spark-worker-2`, etc.

### Reglas Inbound

| Tipo | Puerto | Origen | Motivo |
| :--- | :--- | :--- | :--- |
| SSH | `22` | Tu IP publica o Bastion | Administracion por SSH |
| Custom TCP | `8081` | `SG_PROXY` | UI del worker 1 |
| Custom TCP | `8083` | `SG_PROXY` | UI del worker 2, si existe |
| Custom TCP | `8084` | `SG_PROXY` | UI del worker 3, si existe |
| All TCP | `0-65535` | `SG_SPARK_MASTER` | Comunicacion interna con Master |
| All TCP | `0-65535` | `SG_SPARK_WORKERS` | Comunicacion entre workers |
| All TCP | `0-65535` | `SG_MASTER` | Necesario si Airflow ejecuta el driver del job |

### Reglas Outbound

| Tipo | Puerto | Destino | Motivo |
| :--- | :--- | :--- | :--- |
| Custom TCP | `7077` | `SG_SPARK_MASTER` | Worker se conecta al Master |
| All traffic | All | `0.0.0.0/0` | Descargar imagenes, paquetes, acceder a S3 via NAT |

---

## 4.3 Ajuste Necesario en `SG_MASTER` de Airflow

Si Airflow usa `SparkSubmitOperator` con `conn_id="spark_standalone"`, Airflow debe poder conectarse al Spark Master.

Agrega esta regla outbound en `SG_MASTER`:

| Tipo | Puerto | Destino | Motivo |
| :--- | :--- | :--- | :--- |
| Custom TCP | `7077` | `SG_SPARK_MASTER` | Airflow envia jobs al cluster Spark |

Si los Spark Workers necesitan conectarse de vuelta al proceso driver creado por Airflow, agrega tambien reglas inbound en `SG_MASTER`:

| Tipo | Puerto | Origen | Motivo |
| :--- | :--- | :--- | :--- |
| Custom TCP | `4040` | `SG_SPARK_WORKERS` | Spark Application UI, opcional |
| Custom TCP | `7079` | `SG_SPARK_WORKERS` | Puerto fijo del Spark driver, recomendado |
| Custom TCP | `7080` | `SG_SPARK_WORKERS` | Puerto fijo del block manager, recomendado |

Para que estos puertos sean realmente fijos, tus jobs Spark deben configurar:

```python
.config("spark.driver.port", "7079")
.config("spark.blockManager.port", "7080")
.config("spark.ui.port", "4040")
```

Si no fijas estos puertos, Spark puede usar puertos aleatorios y AWS bloqueara la comunicacion.

---

## 5. Resumen de Reglas Inbound

| Security Group | Puerto | Origen | Proposito |
| :--- | :--- | :--- | :--- |
| `SG_SPARK_MASTER` | `22` | Tu IP o Bastion | SSH |
| `SG_SPARK_MASTER` | `7077` | `SG_MASTER` | Airflow submit |
| `SG_SPARK_MASTER` | `7077` | `SG_SPARK_WORKERS` | Registro de workers |
| `SG_SPARK_MASTER` | `8082` | `SG_PROXY` | Web UI Spark Master |
| `SG_SPARK_MASTER` | All TCP | `SG_SPARK_WORKERS` | Comunicacion interna Spark |
| `SG_SPARK_WORKERS` | `22` | Tu IP o Bastion | SSH |
| `SG_SPARK_WORKERS` | `8081`, `8083`, `8084` | `SG_PROXY` | Web UI de workers |
| `SG_SPARK_WORKERS` | All TCP | `SG_SPARK_MASTER` | Comunicacion interna Spark |
| `SG_SPARK_WORKERS` | All TCP | `SG_SPARK_WORKERS` | Comunicacion entre workers |
| `SG_MASTER` | `7079`, `7080` | `SG_SPARK_WORKERS` | Retorno hacia driver de Spark en Airflow |

---

## 6. Configuracion de la EC2 Spark Master

### 6.1 Crear la EC2

1. Ve a **EC2 > Instances > Launch instances**.
2. **Name:** `spark-master`.
3. **AMI:** Ubuntu Server 24.04 LTS.
4. **Instance type:** `t3.medium` o `t3.large`.
5. **Key pair:** usa la misma llave SSH del proyecto.
6. **VPC:** `data-platform-vpc`.
7. **Subnet:** subred privada, por ejemplo `data-platform-private-subnet`.
8. **Auto-assign public IP:** `Disable`.
9. **Security group:** `SG_SPARK_MASTER`.
10. Lanza la instancia.

### 6.2 Preparar Docker

Conectate por SSH usando Bastion o ProxyJump y ejecuta:

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg git

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker ubuntu
```

Cierra sesion SSH y vuelve a entrar para que el grupo `docker` aplique.

### 6.3 Copiar el proyecto

```bash
git clone <url-del-repositorio>
cd orion_caso4
```

Si ya copiaste el proyecto por `scp`, entra a la carpeta correspondiente.

### 6.4 Configurar `.env` del Spark Master

En `architecture/.env`, para el master deja:

```env
SPARK_IMAGE=orion/spark-s3a:3.5.0
SPARK_NETWORK=spark-net

SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8082

WORKER_ID=1
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_WEBUI_PORT=8081

SPARK_LOGS_PATH=../runtime/logs
SPARK_WORK_PATH=../runtime/work

SPARK_RPC_AUTH=no
SPARK_RPC_ENC=no
SPARK_STORAGE_ENC=no
SPARK_SSL=no
RESTART_POLICY=unless-stopped
```

### 6.5 Levantar Spark Master

Desde la raiz del proyecto:

```bash
docker network create spark-net
mkdir -p architecture/spark_orion/runtime/logs architecture/spark_orion/runtime/work

docker compose \
  --env-file architecture/.env \
  -f architecture/spark_orion/master/docker-compose.master.yml \
  up -d
```

Validar:

```bash
docker ps
docker logs spark-master --tail 100
```

La UI del master debe quedar en:

```text
http://<ip-privada-spark-master>:8082
```

Si usas Nginx Proxy Manager, publica esa IP privada y puerto `8082` con un dominio como:

```text
orion-spark-master.coderhivex.com
```

---

## 7. Configuracion de cada EC2 Spark Worker

### 7.1 Crear la EC2

1. Ve a **EC2 > Instances > Launch instances**.
2. **Name:** `spark-worker-1`.
3. **AMI:** Ubuntu Server 24.04 LTS.
4. **Instance type:** `t3.medium` o `t3.large`.
5. **VPC:** `data-platform-vpc`.
6. **Subnet:** subred privada.
7. **Auto-assign public IP:** `Disable`.
8. **Security group:** `SG_SPARK_WORKERS`.
9. Lanza la instancia.

Repite para `spark-worker-2`, `spark-worker-3`, etc.

### 7.2 Preparar Docker

Instala Docker igual que en el master.

### 7.3 Configurar `.env` del Worker

En cada worker, el valor mas importante es `SPARK_MASTER_HOST`.

Si el worker esta en otra EC2, usa la **IP privada del Spark Master**:

```env
SPARK_IMAGE=orion/spark-s3a:3.5.0
SPARK_NETWORK=spark-net

SPARK_MASTER_HOST=10.0.2.50
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8082

WORKER_ID=1
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_WEBUI_PORT=8081

SPARK_LOGS_PATH=../runtime/logs
SPARK_WORK_PATH=../runtime/work

SPARK_RPC_AUTH=no
SPARK_RPC_ENC=no
SPARK_STORAGE_ENC=no
SPARK_SSL=no
RESTART_POLICY=unless-stopped
```

Cambia `10.0.2.50` por la IP privada real de tu EC2 `spark-master`.

Para varios workers, cambia estos valores:

| Worker | `WORKER_ID` | `SPARK_WORKER_WEBUI_PORT` |
| :--- | :--- | :--- |
| `spark-worker-1` | `1` | `8081` |
| `spark-worker-2` | `2` | `8083` |
| `spark-worker-3` | `3` | `8084` |

### 7.4 Levantar Spark Worker

Desde la raiz del proyecto en cada worker:

```bash
docker network create spark-net
mkdir -p architecture/spark_orion/runtime/logs architecture/spark_orion/runtime/work

docker compose \
  --env-file architecture/.env \
  -f architecture/spark_orion/worker/docker-compose.worker.yml \
  up -d
```

Validar:

```bash
docker ps
docker logs spark-worker-1 --tail 100
```

En los logs debes ver que el worker se registra contra:

```text
spark://<ip-privada-spark-master>:7077
```

---

## 8. Conexion desde Airflow hacia Spark

En Airflow, la conexion `spark_standalone` debe apuntar al Spark Master.

En la interfaz de Airflow:

1. Ve a **Admin > Connections**.
2. Crea o edita la conexion `spark_standalone`.
3. Configura:

| Campo | Valor |
| :--- | :--- |
| Connection Id | `spark_standalone` |
| Connection Type | `Spark` |
| Host | `spark://<ip-privada-spark-master>` |
| Port | `7077` |

Ejemplo:

```text
spark://10.0.2.50:7077
```

Si el provider de Spark espera host sin protocolo, usa:

```text
Host: 10.0.2.50
Port: 7077
Extra: {"deploy-mode": "client"}
```

---

## 9. Reglas para Nginx Proxy Manager

Si quieres ver las interfaces web desde navegador, crea proxy hosts:

### Spark Master UI

| Campo | Valor |
| :--- | :--- |
| Domain | `orion-spark-master.coderhivex.com` |
| Scheme | `http` |
| Forward Hostname / IP | IP privada de `spark-master` |
| Forward Port | `8082` |
| Websockets Support | Activado |

### Spark Worker 1 UI

| Campo | Valor |
| :--- | :--- |
| Domain | `orion-spark-worker-1.coderhivex.com` |
| Scheme | `http` |
| Forward Hostname / IP | IP privada de `spark-worker-1` |
| Forward Port | `8081` |
| Websockets Support | Activado |

### Spark Worker 2 UI

| Campo | Valor |
| :--- | :--- |
| Domain | `orion-spark-worker-2.coderhivex.com` |
| Scheme | `http` |
| Forward Hostname / IP | IP privada de `spark-worker-2` |
| Forward Port | `8083` |
| Websockets Support | Activado |

---

## 10. Checklist de Validacion

### En Spark Master

```bash
docker ps
docker logs spark-master --tail 100
```

Debe aparecer el master escuchando en:

```text
spark://spark-master:7077
```

o en:

```text
spark://<ip-privada>:7077
```

### En cada Worker

```bash
docker ps
docker logs spark-worker-1 --tail 100
```

Debe aparecer registro exitoso contra el master.

### Desde Airflow Master

Prueba conectividad al master:

```bash
nc -vz <ip-privada-spark-master> 7077
```

Si no tienes `nc`:

```bash
sudo apt install -y netcat-openbsd
```

### Desde el navegador

Abre la UI del master:

```text
https://orion-spark-master.coderhivex.com
```

En la pantalla del Spark Master deben aparecer los workers registrados.

---

## 11. Errores Comunes

### El worker no aparece en el Master

Revisar:

- `SPARK_MASTER_HOST` en el `.env` del worker.
- Regla inbound `7077` en `SG_SPARK_MASTER` desde `SG_SPARK_WORKERS`.
- Que ambas EC2 esten en la misma VPC o tengan ruta privada entre ellas.
- Logs del worker con `docker logs spark-worker-1 --tail 100`.

### Airflow no puede enviar jobs

Revisar:

- `SG_MASTER` tiene outbound hacia `SG_SPARK_MASTER:7077`.
- `SG_SPARK_MASTER` tiene inbound `7077` desde `SG_MASTER`.
- La conexion `spark_standalone` en Airflow apunta a `spark://<ip-privada-master>:7077`.

### El job inicia pero falla al conectar executors con driver

Esto pasa cuando Airflow ejecuta el driver y los workers no pueden volver a conectarse a Airflow.

Solucion:

- Abrir inbound en `SG_MASTER` desde `SG_SPARK_WORKERS` a puertos `7079` y `7080`.
- Fijar en el job:

```python
.config("spark.driver.port", "7079")
.config("spark.blockManager.port", "7080")
.config("spark.ui.port", "4040")
```

### No abre la UI de Spark

Revisar:

- Spark Master UI usa `8082`, no `8080`.
- `SG_SPARK_MASTER` permite `8082` desde `SG_PROXY`.
- Nginx Proxy apunta a la IP privada correcta.
- El contenedor esta arriba con `docker ps`.

---

## 12. Configuracion Final Recomendada

Para este proyecto, deja:

| Variable | Master | Worker |
| :--- | :--- | :--- |
| `SPARK_MASTER_HOST` | `spark-master` | IP privada del Spark Master |
| `SPARK_MASTER_PORT` | `7077` | `7077` |
| `SPARK_MASTER_WEBUI_PORT` | `8082` | `8082` |
| `WORKER_ID` | `1` | Unico por worker |
| `SPARK_WORKER_WEBUI_PORT` | `8081` | Unico por worker |
| `SPARK_WORKER_CORES` | `2` | Segun EC2 |
| `SPARK_WORKER_MEMORY` | `2G` o `4G` | Segun EC2 |
| `SPARK_NETWORK` | `spark-net` | `spark-net` |

La regla mas importante: **los workers no deben usar `SPARK_MASTER_HOST=spark-master` si estan en otra EC2**. En ese caso deben usar la IP privada real del Spark Master.
