# Guía de Variables de Entorno (`.env`) de Airflow

El archivo `.env` es el corazón de la configuración de tu plataforma de datos. Airflow lee este archivo para saber cómo conectarse a la base de datos, dónde mandar las tareas, y cómo asegurar la plataforma.

A continuación, se explica cada bloque de tu archivo `.env` actual y qué máquina lo utiliza.

---

## 1. Explicación detallada por bloques

### 🛠️ CORE (Núcleo)
*   **`PYTHONPATH=/opt/pipelines`**: Le dice a Python en qué carpeta buscar tu código personalizado y los DAGs.
*   **`AIRFLOW_UID=50000`**: El ID del usuario Linux dentro del contenedor de Docker. Por seguridad, Airflow no corre como root.

### 👷 WORKER CONFIG (Configuración de Trabajadores)
*   **`CELERY_HOSTNAME=proxy`** ⚠️ *ATENCIÓN*: El valor `proxy` aquí es incorrecto para los workers. Esta variable define cómo se llama el worker en el panel de Flower. En el Master puede quedar así, pero en la máquina `worker1`, este valor debería ser `worker1`, en el `worker2` debería ser `worker2`, etc.
*   **`QUEUES=default`**: Define qué fila de tareas (queue) va a escuchar este worker.

### ⚙️ AIRFLOW CORE (Configuración Principal)
*   **`AIRFLOW__CORE__EXECUTOR=CeleryExecutor`**: La magia de la arquitectura distribuida. Le dice a Airflow que no corra las tareas localmente, sino que las mande a RabbitMQ para que los workers las recojan.
*   **`AIRFLOW__CORE__FERNET_KEY`**: Una llave criptográfica súper secreta que Airflow usa para encriptar las contraseñas que guardes en la base de datos (por ejemplo, claves de bases de datos externas).
*   **`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`**: Hace que cuando subas un nuevo DAG, aparezca apagado por defecto para que no se ejecute por accidente.
*   **`AIRFLOW__CORE__AUTH_MANAGER`**: Activa el gestor de usuarios estándar de Airflow (FAB - Flask App Builder).

### 🗄️ DATABASE (Base de Datos)
*   **`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`**: La cadena de conexión a tu PostgreSQL en la instancia proxy (`23.0.1.199`). Aquí Airflow guarda todo: qué tareas fallaron, usuarios, permisos, etc.

### 🐇 CELERY (Mensajería)
*   **`AIRFLOW__CELERY__BROKER_URL`**: La dirección de tu **RabbitMQ** (`amqp://23.0.2.75:5672`). Es el "buzón de correos" donde el Master deja las tareas y los Workers las recogen.
*   **`AIRFLOW__CELERY__RESULT_BACKEND`**: Donde Celery guarda el resultado final de la tarea (apunta a la misma base de datos PostgreSQL).

### 🌐 API / WEB (Servidor Web)
*   **`AIRFLOW__API__BASE_URL`** y **`AIRFLOW__CORE__EXECUTION_API_SERVER_URL`**: Tu dominio público de Nginx (`http://orion-airflow.coderhivex.com`). Airflow usa esto para generar enlaces correctos en los correos y validar tokens.
*   **`AIRFLOW__API_AUTH__JWT_SECRET`**: Clave secreta para firmar los "tokens" de inicio de sesión de los usuarios en la API.
*   **`AIRFLOW__API__SERVER_TYPE=gunicorn`**: Le dice a la interfaz web que use Gunicorn (un servidor robusto para producción) en vez del servidor básico de pruebas.

### ⏱️ SCHEDULER (Planificador)
*   **`AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true`**: Activa un endpoint para que sepas si el scheduler está vivo.
*   **`AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=300`**: Cada cuántos segundos Airflow revisa tu carpeta de código en busca de DAGs nuevos o modificados (300s = 5 minutos).

### 🚀 CLI / INIT (Inicialización)
*   **`_AIRFLOW_DB_MIGRATE=false`**: Evita que Airflow intente crear las tablas de la base de datos cada vez que arranca. Solo debe estar en `true` en el contenedor de inicialización (`airflow-init`).
*   **`_AIRFLOW_WWW_USER_...`**: Son los datos de tu usuario administrador (`orion.riwi`). Airflow crea este usuario la primera vez que se instala.

### ☁️ LOGGING (Registros en S3)
*   **`AIRFLOW__LOGGING__REMOTE_LOGGING=true`**: Le dice a Airflow que no guarde los logs de las tareas localmente, sino que los envíe a la nube.
*   **`AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER`**: El nombre de tu bucket en AWS S3 (`s3://orion-s3-.../orion-logs`).
*   **`AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=my_s3_orion`**: El nombre de la conexión que creaste en la interfaz web con tus Access Keys de AWS.

### 📦 EXTRA REQUIREMENTS (Dependencias Extras)
*   **`EXTRA_REQUIREMENTS`**: Lista de paquetes de Python (como `pandas`, `openpyxl`, `gunicorn` y el proveedor de Amazon) que Docker instalará automáticamente al encenderse.

---

## 2. Variables de Entorno por Máquina

Aunque la buena práctica es mantener el mismo archivo `.env` completo en todas las máquinas para evitar desincronizaciones, técnicamente cada servicio solo utiliza un subconjunto específico de variables. Aquí tienes el desglose exacto:

### 1. Nginx Proxy Manager (Instancia `proxy-server`)
*   **NO USA ESTE ARCHIVO.** Nginx se configura a través de su propia interfaz web (puerto 81). No lee el `.env` de Airflow.

### 2. RabbitMQ (Instancia `rabbitmq-server`)
*   **NO USA ESTE ARCHIVO.** RabbitMQ usa las variables de su propio `docker-compose.yml` (ej. `RABBITMQ_DEFAULT_USER`).

### 3. Airflow Master (Instancia `airflow-master`)
Esta máquina ejecuta el Webserver, el Scheduler, el Triggerer y Flower. Por lo tanto, usa el 100% del archivo `.env`:

*   **Generales y Base de Datos:**
    *   `PYTHONPATH`, `AIRFLOW_UID`
    *   `AIRFLOW__CORE__EXECUTOR`
    *   `AIRFLOW__CORE__FERNET_KEY`
    *   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
*   **Mensajería (Celery y Flower):**
    *   `AIRFLOW__CELERY__BROKER_URL`
    *   `AIRFLOW__CELERY__RESULT_BACKEND`
*   **Interfaz Web y API:**
    *   `AIRFLOW__API__BASE_URL`
    *   `AIRFLOW__API__INSTANCE_NAME`
    *   `AIRFLOW__API__SECRET_KEY`
    *   `AIRFLOW__API__SERVER_TYPE`
    *   `AIRFLOW__API_AUTH__JWT_SECRET`
    *   `AIRFLOW__API_AUTH__JWT_ISSUER`
    *   `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`
*   **Scheduler y Comandos Base:**
    *   `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK`
    *   `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL`
    *   `_AIRFLOW_DB_MIGRATE`
    *   `_AIRFLOW_WWW_USER_...` (Solo para inicialización).
*   **Logging:**
    *   `AIRFLOW__LOGGING__...` (Todas las variables de remote logging para leer los logs desde S3).
*   **Dependencias Adicionales:**
    *   `EXTRA_REQUIREMENTS`

### 4. Airflow Workers (Instancias `worker1`, `worker2`, `worker3`)
Los workers son "ciegos" a la interfaz web. Solo se dedican a ejecutar el código de tus DAGs y reportar si funcionó o falló a RabbitMQ y Postgres.

*   **Obligatorias (Identidad y Conexión):**
    *   `CELERY_HOSTNAME` **(⚠️ Única variable que debe cambiar. Debe ser `worker1`, `worker2`, etc. en cada máquina).**
    *   `QUEUES=default`
    *   `PYTHONPATH`, `AIRFLOW_UID`
    *   `AIRFLOW__CORE__EXECUTOR`
    *   `AIRFLOW__CORE__FERNET_KEY`
*   **Comunicación con la Arquitectura:**
    *   `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (Para reportar los estados de éxito/fallo a Postgres).
    *   `AIRFLOW__CELERY__BROKER_URL` (Para consumir y escuchar tareas nuevas desde RabbitMQ).
    *   `AIRFLOW__CELERY__RESULT_BACKEND`
*   **Logging (Para escribir resultados en la nube):**
    *   `AIRFLOW__LOGGING__REMOTE_LOGGING`
    *   `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER`
    *   `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID`
*   **Dependencias Adicionales:**
    *   `EXTRA_REQUIREMENTS` (Fundamental para descargar librerías como Pandas o el conector de S3 de Amazon).

*(Nota: Aunque los workers no usan en absoluto las variables de `API / WEB` ni del `SCHEDULER`, la recomendación oficial es dejarlas en el archivo de los workers para que todos los nodos tengan la misma configuración base; simplemente las ignorarán).*
