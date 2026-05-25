# Automatización de Instalación de Dependencias vía SSH

Ya que tienes tu archivo `~/.ssh/config` configurado con alias (`master`, `rabbitmq`, `worker1`, etc.), puedes aprovecharlo para instalar Docker, Git y Docker Compose en todas tus máquinas usando un único script automatizado.

---

## 1. Crear el Script Unificado (`install_all.sh`)

Crearemos un único archivo que iterará sobre todas tus instancias y les enviará las instrucciones de instalación sin que tengas que crear archivos adicionales.

Desde la terminal de tu máquina base (el Proxy o Master), crea el archivo:

```bash
nano install_all.sh
```

Copia y pega este contenido exacto:

```bash
#!/bin/bash

# Lista de todos los alias que configuraste en tu ~/.ssh/config
SERVIDORES=("master" "rabbitmq" "worker1" "worker2" "worker3")

for servidor in "${SERVIDORES[@]}"
do
  echo "=========================================================="
  echo "🚀 INICIANDO INSTALACIÓN EN: $servidor"
  echo "=========================================================="
  
  # Usamos << 'EOF' para enviar todo este bloque de comandos al servidor remoto
  ssh $servidor << 'EOF'
export DEBIAN_FRONTEND=noninteractive

echo "Actualizando sistema..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release git

echo "Descargando llaves de Docker..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg --yes
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "Agregando repositorio de Docker..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "Instalando Docker y herramientas..."
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "Configurando permisos de usuario..."
sudo usermod -aG docker ubuntu

echo "Verificando instalación..."
docker --version
docker compose version
git --version
EOF

  echo "✅ INSTALACIÓN COMPLETADA EN: $servidor"
  echo ""
done

echo "🎉 ¡Todas las instancias han sido configuradas!"
```

Guarda y cierra (`Ctrl + O`, `Enter`, `Ctrl + X`).

---

## 2. Ejecutar la automatización

Antes de correrlo, dale permisos de ejecución a tu script:

```bash
chmod +x install_all.sh
```

Finalmente, ejecuta el script con un solo comando:

```bash
./install_all.sh
```

### ¿Qué sucederá?
Verás en tu pantalla cómo la terminal se conecta automáticamente al `master`, luego a `rabbitmq`, y a cada uno de los `workers`. Instalando de manera desatendida Docker, Docker Compose y Git. Al finalizar, tendrás toda tu infraestructura base lista para descargar y correr los contenedores de Airflow.

*(Nota: Aunque el script agrega a tu usuario `ubuntu` al grupo `docker`, si después intentas conectarte manualmente a un worker y lanzar comandos de docker, recuerda salir y volver a entrar (`exit` y luego `ssh worker1`) para que Linux reconozca los nuevos permisos del grupo).*
