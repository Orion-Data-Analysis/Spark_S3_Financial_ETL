# Configuración de Nginx Proxy Manager (NPM)

Esta guía documenta cómo configurar los dominios públicos de tu plataforma de datos y cómo aplicar reglas avanzadas de Nginx para asegurar que los WebSockets (usados para ver logs en tiempo real) y las redirecciones funcionen correctamente.

Nginx Proxy Manager maneja el enrutamiento a través de su interfaz visual. A continuación se explica cómo configurar cada dominio usando los bloques `location` que has definido.

---

## 1. Configuración del Dominio de Airflow
**Dominio:** `orion-airflow.coderhivex.com`

1. Entra a Nginx Proxy Manager (puerto 81).
2. Ve a **Hosts** > **Proxy Hosts** y haz clic en **Add Proxy Host**.
3. En la pestaña **Details**, configura:
   *   **Domain Names:** `orion-airflow.coderhivex.com`
   *   **Scheme:** `http`
   *   **Forward Hostname / IP:** `23.0.2.251` (IP de tu Master)
   *   **Forward Port:** `8080`
   *   **Block Common Exploits:** ✅ Activado
   *   **Websockets Support:** ✅ Activado
4. Ve a la pestaña **Advanced** y pega la siguiente configuración para forzar el enrutamiento seguro de encabezados y WebSockets:

```nginx
location / {
    proxy_pass http://23.0.2.251:8080/;
    
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    # Importante para los WebSockets de Airflow (logs en tiempo real)
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```
5. *(Opcional)* En la pestaña **SSL**, solicita un certificado y marca `Force SSL`. Guarda los cambios.

---

## 2. Configuración del Dominio de Flower
**Dominio:** `orion-flower.coderhivex.com`

1. Haz clic en **Add Proxy Host**.
2. En la pestaña **Details**, configura:
   *   **Domain Names:** `orion-flower.coderhivex.com`
   *   **Scheme:** `http`
   *   **Forward Hostname / IP:** `23.0.2.251` (IP de tu Master)
   *   **Forward Port:** `5555`
   *   **Websockets Support:** ✅ Activado
3. Ve a la pestaña **Advanced** y pega la siguiente configuración:

```nginx
location / {
    proxy_pass http://23.0.2.251:5555/; 
    
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    
    proxy_redirect off;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```
4. Guarda los cambios.

---

## 3. Configuración del Dominio de RabbitMQ
**Dominio:** `orion-rabbitmq.coderhivex.com`

1. Haz clic en **Add Proxy Host**.
2. En la pestaña **Details**, configura:
   *   **Domain Names:** `orion-rabbitmq.coderhivex.com`
   *   **Scheme:** `http`
   *   **Forward Hostname / IP:** `23.0.2.75` (IP Privada de tu servidor RabbitMQ)
   *   **Forward Port:** `15672`
   *   **Websockets Support:** ✅ Activado
3. Ve a la pestaña **Advanced** y pega la configuración para RabbitMQ. *(Nota: La reescritura de ruta (`rewrite`) es esencial para la API interna de RabbitMQ)*:

```nginx
location / {
    rewrite ^/(.*)$ /$1 break;
    proxy_pass http://23.0.2.75:15672/;
    proxy_http_version 1.1;

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";

    proxy_redirect off;
}
```
4. Guarda los cambios.

---

### 💡 Nota sobre las rutas (`location /`)
Como decidiste utilizar **un subdominio dedicado para cada servicio** (ej. `orion-flower` y `orion-airflow`), la regla de `location` en Nginx debe ser la ruta raíz `/` en lugar de una ruta anidada como `/airflow/`. 

Si usaras un solo dominio para todo (por ejemplo, `miservidor.com`), entonces sí necesitarías usar `location /airflow/`, `location /flower/` y `location /rabbitmq/` dentro del mismo bloque Advanced. Sin embargo, usar subdominios independientes es la mejor práctica recomendada para aislar la seguridad de las cookies y las interfaces.
