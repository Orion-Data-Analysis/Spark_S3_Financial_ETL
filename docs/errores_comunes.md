# 🛠️ Errores Comunes y Soluciones

Durante el despliegue de esta plataforma distribuida en la nube, nos encontramos con algunos obstáculos técnicos reales relacionados con la red, las bases de datos y los servidores de producción. 

A continuación, documentamos detalladamente cómo diagnosticamos y solucionamos los tres problemas principales para que sirva de guía de supervivencia.

---

## 1. Error 502 Bad Gateway (Airflow Webserver)

**El Síntoma:** 
Al intentar ingresar a la interfaz web de Airflow a través del dominio configurado en Nginx Proxy Manager, el navegador mostraba una pantalla genérica de `502 Bad Gateway`. 
Al inspeccionar los logs del contenedor Master (`docker logs airflow-webserver`), encontramos este error interno en Python:
```text
ModuleNotFoundError: No module named 'gunicorn'
```

**La Causa:** 
En nuestro archivo `.env`, le dijimos explícitamente a Airflow que usara Gunicorn (`AIRFLOW__API__SERVER_TYPE=gunicorn`) para que el servidor web sea estable en producción. Sin embargo, la imagen de Docker oficial de Airflow a veces no trae esta librería instalada de forma predeterminada en el núcleo del sistema, por lo que el contenedor fallaba inmediatamente al intentar encender.

**La Solución Detallada:** 
Tuvimos que inyectar la dependencia directamente en la etapa de construcción de la imagen de Docker.

1. Abrimos el archivo `Dockerfile` ubicado en la carpeta `master/`.
2. Editamos la línea de instalación (`RUN pip install ...`) agregando explícitamente el paquete `gunicorn` y el extra de airflow `[gunicorn]`. El archivo quedó así:
   ```dockerfile
   FROM apache/airflow:2.8.1
   USER root
   # ... [otras dependencias del SO] ...
   USER airflow
   RUN pip install --no-cache-dir 'apache-airflow-core[gunicorn]' gunicorn apache-airflow-providers-amazon pandas
   ```
3. También lo agregamos a la variable `EXTRA_REQUIREMENTS` en el archivo `.env`.
4. Finalmente, reconstruimos la imagen para aplicar los cambios:
   ```bash
   docker compose build
   docker compose up -d
   ```

---

## 2. Bloqueo de Base de Datos (`psycopg2.errors.LockNotAvailable`)

**El Síntoma:** 
Al arrancar el entorno por primera vez, el contenedor de inicialización (`airflow-init`) entraba en un ciclo infinito de reinicios. En sus logs se veía un bloqueo severo de PostgreSQL:
```text
sqlalchemy.exc.OperationalError: (psycopg2.errors.LockNotAvailable) 
relation "alembic_version" does not exist
```

**La Causa:** 
Este problema ocurre por una "condición de carrera" (Race Condition). En una arquitectura donde tienes múltiples instancias (Workers, Webserver, Scheduler), todas intentan encenderse a la vez. Cuando arrancan, todas intentan conectarse a la Base de Datos y ejecutar migraciones (crear tablas faltantes) **al mismo tiempo**. 
PostgreSQL, para protegerse de que dos procesos dañen la estructura, "bloquea" la base de datos (Lock), pero debido al choque masivo, el bloqueo se queda colgado para siempre.

**La Solución Detallada:**
Para solucionarlo tuvimos que limpiar el bloqueo y prevenir que volviera a ocurrir.

1. **Paso 1: Destrabar la base de datos (PostgreSQL).**
   Nos conectamos a nuestra base de datos externa usando un cliente SQL (como DBeaver o pgAdmin) y ejecutamos la siguiente consulta de rescate:
   ```sql
   SELECT pg_advisory_unlock_all();
   ```
   *Esto forzó a Postgres a liberar todas las tablas trabadas.*

2. **Paso 2: Apagar el caos concurrente.**
   Bajamos todos los servicios de Docker:
   ```bash
   docker compose down
   ```

3. **Paso 3: Prevenir el error en el código.**
   Modificamos el archivo `.env` para asegurarnos de que la variable de auto-migración estuviera estrictamente apagada:
   ```env
   _AIRFLOW_DB_MIGRATE=false
   ```
   De esta forma, solo el contenedor `airflow-init` tiene permiso para hacer migraciones una vez, mientras que los Workers y el Webserver simplemente esperan a que él termine, eliminando la condición de carrera.

---

## 3. Error 504 Gateway Time-out (RabbitMQ y Flower)

**El Síntoma:** 
Teníamos RabbitMQ y Flower corriendo perfectamente (podíamos confirmarlo usando `docker ps` y `curl localhost:15672` dentro de la máquina respectiva). Sin embargo, cuando intentábamos entrar desde el navegador (Nginx Proxy Manager), la pantalla se quedaba cargando durante un minuto y finalmente devolvía `504 Gateway Time-out`.

**La Causa:** 
Un error 502 significa que Nginx llegó al servidor pero la aplicación falló. Sin embargo, un **504 significa que Nginx no pudo alcanzar la máquina de destino**, es decir, el servidor jamás respondió.
Esto nos indicó que el problema no era de código ni de Docker, sino de **Infraestructura (Firewall)**. El Proxy, al estar en una subred pública, estaba intentando conectarse a las instancias privadas, pero AWS estaba rechazando la conexión.

**La Solución Detallada:**
Tuvimos que corregir las reglas de red en la nube de Amazon (AWS).

1. Entramos a la consola de **AWS EC2**.
2. Fuimos al panel izquierdo: **Security Groups** (Grupos de Seguridad).
3. Buscamos el grupo asignado a la máquina problemática (por ejemplo, `sg-rabbitmq` o `sg-master` para Flower).
4. En la pestaña **Inbound Rules** (Reglas de entrada), hicimos clic en **Edit Inbound Rules**.
5. Agregamos las reglas específicas que faltaban:
   * **Tipo:** Custom TCP
   * **Puerto:** `15672` (Panel de RabbitMQ) o `5555` (Flower)
   * **Origen (Source):** Aquí es clave el error. En lugar de poner una IP fija, escribimos el nombre del Grupo de Seguridad del Proxy (`sg-proxy`).
6. Guardamos los cambios. El impacto fue inmediato: AWS autorizó el flujo de red interna y el error 504 desapareció al instante.
