# Sprint Backlog: Habilitación de Infraestructura y Conectividad del Data Lake (Sprint 1)

**ÉPICA:** Implementación del Data Lake en AWS S3 e Integración de la Plataforma de Datos

---

## 🎫 Historia de Usuario 1: Configuración del Almacenamiento Seguro del Data Lake en S3
**Puntuación sugerida:** 3 Story Points  
**Responsable:** Data Engineer / Cloud Engineer  

### 📄 Descripción
Como Ingeniero de Datos, quiero disponer de un bucket de Amazon S3 estructurado y protegido para almacenar las capas lógicas del Data Lake (`landing`, `raw`, `staging`, `intermediate`, `mart`, `consumption`), garantizando la persistencia, el aislamiento por entornos y la trazabilidad de los datos financieros del proyecto Orion.

### 📋 Criterios de Aceptación (Definition of Done)
- [ ] El bucket S3 está creado exclusivamente en la región `us-east-2` (Ohio).
- [ ] La configuración de bloqueo de acceso público (*Block all public access*) está activada al 100%.
- [ ] El versionamiento de objetos (*Bucket Versioning*) está habilitado.
- [ ] El cifrado en reposo está activo utilizando llaves administradas por S3 (SSE-S3).
- [ ] La propiedad *Bucket Key* está configurada en **Enable** para optimizar costos transaccionales.
- [ ] La estructura de directorios raíz (`dev/financial_crisis/`, `qa/`, `prod/`) junto con sus capas lógicas internas es visible y navegable desde la consola web de AWS.

### 🛠️ Subtareas Técnicas Detalladas

#### 🔹 Subtarea 1.1: Creación y Hardening del Bucket S3 en la Consola
1. Iniciar sesión en la Consola de AWS y navegar al servicio de **S3**.
2. Hacer clic en el botón naranja **Create bucket**.
3. Configurar los siguientes parámetros mandatorios:
   - **Bucket name:** `orion-financial-crisis-data-395840094505-us-east-2-an` *(Nombre global único verificado)*.
   - **AWS Region:** `us-east-2` (US East (Ohio)).
   - **Object Ownership:** *ACLs disabled (recommended)*.
4. En **Block Public Access settings for this bucket**, asegurar que la casilla *Block all public access* esté totalmente seleccionada.
5. En **Bucket Versioning**, cambiar el estado a **Enable**.
6. En **Default encryption**:
   - **Encryption type:** *Server-side encryption with Amazon S3 managed keys (SSE-S3)*.
   - **Bucket Key:** Seleccionar **Enable**.
7. Dejar los demás campos por defecto y hacer clic en **Create bucket**.

#### 🔹 Subtarea 1.2: Inicialización de la Estructura Base de Carpetas Generales
1. Con el bucket creado con éxito, hacer clic en el ícono de **AWS CloudShell** `[ >_ ]` ubicado en la barra de herramientas superior derecha de la consola de AWS.
2. Esperar a que el entorno de la terminal Linux se inicialice por completo.
3. Copiar y pegar el siguiente bloque de comandos CLI en la consola de CloudShell para inyectar los archivos marcadores `.keep` que darán consistencia visual a las carpetas lógicas:
   ```bash
   # Definir variables de entorno locales en CloudShell
   export BUCKET="orion-financial-crisis-data-395840094505-us-east-2-an"
   touch .keep

   echo "=== Creando Entornos Principales ==="
   aws s3 cp .keep s3://$BUCKET/dev/financial_crisis/.keep
   aws s3 cp .keep s3://$BUCKET/qa/.keep
   aws s3 cp .keep s3://$BUCKET/prod/.keep

   echo "=== Creando Capas Generales dentro de DEV ==="
   export BASE="s3://$BUCKET/dev/financial_crisis"
   aws s3 cp .keep $BASE/landing/.keep
   aws s3 cp .keep $BASE/raw/.keep
   aws s3 cp .keep $BASE/staging/.keep
   aws s3 cp .keep $BASE/intermediate/.keep
   aws s3 cp .keep $BASE/mart/.keep
   aws s3 cp .keep $BASE/consumption/.keep
   aws s3 cp .keep $BASE/manifests/.keep
   aws s3 cp .keep $BASE/dbt/.keep
   aws s3 cp .keep $BASE/logs/.keep

   # Limpieza del entorno local temporal
   rm .keep
   echo "=== ¡Estructura general inicializada con éxito! ==="
   ```
4. Presionar **Enter** y verificar que todos los uploads retornen estado `success`.
5. Regresar a la UI gráfica de S3, recargar la vista y comprobar la existencia ordenada de la taxonomía del Data Lake.

---

## 🎫 Historia de Usuario 2: Gestión de Accesos y Seguridad IAM para la Plataforma de Datos
**Puntuación sugerida:** 3 Story Points  
**Responsable:** Cloud Security Engineer / DevOps  

### 📄 Descripción
Como Administrador de Seguridad de Datos, quiero implementar una política y un rol de IAM basados estrictamente en el principio de menor privilegio, para que los recursos de cómputo (instancias EC2 de Airflow Máster, Workers y Spark) interactúen con el bucket de S3 de manera automática y segura, mitigando el riesgo de filtración de credenciales estáticas.

### 📋 Criterios de Aceptación (Definition of Done)
- [ ] Se ha creado una política IAM inline/personalizada que restringe el acceso únicamente al bucket del proyecto.
- [ ] Se ha creado un IAM Role con política de confianza exclusiva para el servicio de Amazon EC2.
- [ ] La política personalizada está adjunta al nuevo rol.
- [ ] El rol está asignado como perfil de instancia (IAM Instance Profile) en los servidores activos de Airflow y Spark.

### 🛠️ Subtareas Técnicas Detalladas

#### 🔹 Subtarea 2.1: Creación de la Política IAM Personalizada
1. En la consola de AWS, buscar y abrir el servicio **IAM (Identity and Access Management)**.
2. En el panel de navegación izquierdo, hacer clic en **Policies** y posteriormente en el botón **Create policy**.
3. En el editor de políticas, seleccionar la pestaña **JSON**.
4. Borrar el código de ejemplo existente y pegar exactamente el siguiente bloque de control de accesos:
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
5. Hacer clic en **Next**.
6. En el campo **Policy name**, asignar el nombre `orion-data-platform-s3-policy`.
7. Verificar que las acciones mapeen correctamente a S3 y hacer clic en **Create policy**.

#### 🔹 Subtarea 2.2: Creación del IAM Role y Asociación a Servidores EC2
1. En el panel izquierdo de IAM, hacer clic en **Roles** y luego en **Create role**.
2. En **Trusted entity type**, seleccionar **AWS service**. En el menú desplegable de **Service or use case**, elegir **EC2** y hacer clic en **Next**.
3. En la pantalla de políticas de permisos, usar la barra de búsqueda para localizar `orion-data-platform-s3-policy`.
4. Marcar la casilla de verificación situada a la izquierda de la política y pulsar **Next**.
5. En el campo **Role name**, ingresar exactamente `orion-data-platform-s3-role`.
6. Validar en el resumen que la política esté adjunta y hacer clic en **Create role**.
7. Ir a la consola de **EC2 > Instances** y seleccionar el/los servidores donde operan Airflow y Spark.
8. Hacer clic en el menú superior **Actions > Security > Modify IAM role**.
9. Seleccionar `orion-data-platform-s3-role` de la lista desplegable y guardar los cambios haciendo clic en **Update IAM role**.

---

## 🎫 Historia de Usuario 3: Conexión de la Orquestación (Airflow) con el Data Lake S3
**Puntuación sugerida:** 5 Story Points  
**Responsable:** Data Engineer / Analytics Engineer  

### 📄 Descripción
Como Desarrollador de Pipelines, quiero conectar la plataforma Apache Airflow con Amazon S3 utilizando la autenticación implícita del IAM Role asignado, con el fin de parametrizar las variables de entorno del flujo de datos, desvincular el filesystem local `/opt/data/` e implementar la centralización de logs de tareas en la nube.

### 📋 Criterios de Aceptación (Definition of Done)
- [ ] Una conexión segura tipo Amazon Web Services (`aws_orion_s3`) está dada de alta en Airflow sin contraseñas explícitas.
- [ ] El archivo de configuración `.env` de producción/desarrollo está actualizado con el direccionamiento nativo de S3.
- [ ] El mecanismo de Logs Remotos de Airflow apunta directamente al prefijo de S3 correspondiente.

### 🛠️ Subtareas Técnicas Detalladas

#### 🔹 Subtarea 3.1: Configurar Variables de Entorno en el Ecosistema Airflow
1. Acceder por SSH o mediante terminal de desarrollo al servidor de Airflow.
2. Abrir el archivo de configuración global `.env` del proyecto.
3. Añadir/modificar las siguientes variables mapeadas al bucket del proyecto:
   ```plaintext
   FINANCIAL_ENV=dev
   FINANCIAL_AWS_REGION=us-east-2
   FINANCIAL_S3_BUCKET=orion-financial-crisis-data-395840094505-us-east-2-an
   FINANCIAL_S3_DOMAIN=financial_crisis
   FINANCIAL_S3_BASE=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis

   FINANCIAL_INPUT_PATH=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/landing
   FINANCIAL_DATA_LAKE_ROOT=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis
   ```
4. Guardar los cambios del archivo.

#### 🔹 Subtarea 3.2: Configuración de la Conexión en Airflow UI y Enrutamiento de Logs
1. Ingresar a la interfaz web de Airflow.
2. Navegar en el menú superior a **Admin > Connections** y hacer clic en el botón de suma **+**.
3. Rellenar los campos con los siguientes valores exactos:
   - **Connection Id:** `aws_orion_s3`
   - **Connection Type:** `Amazon Web Services`
   - **Extra:**
     ```json
     {
       "region_name": "us-east-2"
     }
     ```
4. Dejar vacíos los campos **AWS Access Key ID** y **AWS Secret Access Key**, ya que Airflow asumirá las credenciales dinámicas gracias al IAM Role asignado a la EC2.
5. Hacer clic en **Save**.
6. Para redirigir los logs de ejecución, abrir el archivo `airflow.cfg` o inyectar como variables de entorno de Airflow la configuración de logging remoto:
   ```plaintext
   AIRFLOW__LOGGING__REMOTE_LOGGING=true
   AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/airflow
   AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_orion_s3
   ```
7. Reiniciar por completo el backend de Airflow.

---

## 🎫 Historia de Usuario 4: Habilitación del Conector Spark-S3 (Protocolo `s3a://`) para Procesamiento Big Data
**Puntuación sugerida:** 5 Story Points  
**Responsable:** Data Engineer / Big Data Specialist  

### 📄 Descripción
Como Ingeniero de Big Data, de modo que el motor de Apache Spark pueda leer formatos origen en `landing` y persistir la data procesada en la capa `raw` en formato Parquet, necesito integrar las librerías nativas de Hadoop AWS para dar soporte al protocolo de alta eficiencia de sistemas de archivos distribuidos `s3a://`.

### 📋 Criterios de Aceptación (Definition of Done)
- [ ] Los paquetes java binarios `.jar` de interconectividad de AWS están instalados en el directorio nativo de dependencias de Spark.
- [ ] El constructor del `SparkSession` hereda de forma obligatoria las propiedades del perfil de instancia de AWS.
- [ ] No existen excepciones en tiempo de ejecución asociadas a clases no encontradas (`ClassNotFoundException`) del ecosistema Hadoop Filesystem.

### 🛠️ Subtareas Técnicas Detalladas

#### 🔹 Subtarea 4.1: Descarga e Instalación de Jars en el Nodo/Contenedor de Spark
1. Validar la versión de Apache Spark y la versión interna de Hadoop corriendo en la infraestructura corporativa.
2. Acceder al repositorio central de Maven y descargar las versiones compatibles de las siguientes librerías:
   - `hadoop-aws-X.X.X.jar`
   - `aws-java-sdk-bundle-X.X.X.jar`
3. Mover/copiar ambos archivos `.jar` descargados dentro del directorio de librerías compartidas de Apache Spark en todos los nodos (Master y Workers):
   - **Ruta estándar:** `/opt/bitnami/spark/jars/`
4. Asegurar que los archivos tengan permisos de lectura para el usuario que ejecuta Spark.

#### 🔹 Subtarea 4.2: Estandarización de la Configuración de Inicialización de PySpark
1. Modificar el bloque base de instanciación del objeto de Spark en el repositorio de scripts del proyecto, asegurando que contenga las siguientes llaves de configuración de Hadoop:
   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder \
       .appName("Orion-Financial-Crisis-DataLake-Ingestion") \
       .config("spark.sql.session.timeZone", "UTC") \
       .config("spark.sql.shuffle.partitions", "8") \
       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
       .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
       .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com") \
       .getOrCreate()
   ```
2. Realizar commit del script modificado al repositorio Git.

---

## 🎫 Historia de Usuario 5: Pruebas de Humo (Smoke Tests) y Validación de Conectividad End-to-End
**Puntuación sugerida:** 2 Story Points  
**Responsable:** Data Engineer / QA Analyst  

### 📄 Descripción
Como Líder Técnico de Datos, quiero ejecutar un set de pruebas rápidas automatizadas en el entorno integrado, con la finalidad de validar que las reglas de red, los roles IAM de AWS y las credenciales implícitas de Airflow y Spark funcionan correctamente antes de iniciar la construcción lógica de los pipelines en el siguiente Sprint.

### 📋 Criterios de Aceptación (Definition of Done)
- [ ] El comando de AWS CLI responde exitosamente desde la terminal interna del servidor sin requerir archivos de credenciales físicas localmente.
- [ ] Airflow interactúa con el bucket S3 de forma nativa a través de un script de prueba usando `S3Hook`.
- [ ] Un Job básico de Spark escribe un dataset sintético en la ruta `s3a://` y realiza una lectura de retorno exitosa sin pérdida de consistencia.

### 🛠️ Subtareas Técnicas Detalladas

#### 🔹 Subtarea 5.1: Test de Conectividad de Sistema e Integración Airflow
1. Conectarse al servidor EC2 por medio de SSH.
2. Ejecutar el comando para comprobar el rol del sistema:
   ```bash
   aws sts get-caller-identity
   ```
3. Validar que la respuesta contenga el ARN del rol creado: `arn:aws:iam::395840094505:role/orion-data-platform-s3-role`.
4. Iniciar el entorno interactivo de Python asociado a tu instalación de Airflow y ejecutar el siguiente bloque para evaluar la conectividad del Hook de AWS:
   ```python
   from airflow.providers.amazon.aws.hooks.s3 import S3Hook

   try:
       hook = S3Hook(aws_conn_id='aws_orion_s3')
       bucket_name = 'orion-financial-crisis-data-395840094505-us-east-2-an'
       if hook.check_for_bucket(bucket_name):
           print(f"ÉXITO: Airflow tiene comunicación fluida con el bucket {bucket_name}")
       else:
           print("ERROR: El bucket no fue localizado.")
   except Exception as e:
       print(f"FALLO DE CONEXIÓN: {str(e)}")
   ```

#### 🔹 Subtarea 5.2: Ejecución del Job de Validación E2E en Spark
1. Crear un script básico de prueba en Python (`test_s3_spark.py`) con la configuración de `SparkSession` detallada en la Historia de Usuario 4.
2. Agregar lógica para generar un dataframe pequeño en memoria y forzar una escritura y lectura en S3:
   ```python
   # Crear un DataFrame de prueba simple
   data = [("digital_accounts", "success", "2026-05-21"), ("virtual_wallets", "pending", "2026-05-21")]
   columns = ["source_system", "status", "date"]
   df = spark.createDataFrame(data, columns)

   # Ruta de destino utilizando el protocolo s3a
   target_path = "s3a://orion-financial-crisis-data-395840094505-us-east-2-an/dev/financial_crisis/logs/test_spark_e2e"

   print("Iniciando test de escritura en S3...")
   df.write.mode("overwrite").parquet(target_path)
   print("Escritura completada exitosamente.")

   print("Iniciando test de lectura desde S3...")
   df_read = spark.read.parquet(target_path)
   df_read.show()
   print("=== PRUEBA END-TO-END DE SPARK CON AWS S3 EXITOSA ===")
   ```
3. Ejecutar el script mediante `spark-submit test_s3_spark.py` y validar que el dataframe se imprima en consola sin generar excepciones de seguridad ni de red.
