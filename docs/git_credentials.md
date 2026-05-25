# Configuración de Credenciales de Git

Si estás trabajando en tus servidores (Master o Workers) y Git te pide el usuario y contraseña cada vez que haces un `git pull`, `git clone` o `git push`, puedes configurarlo para que lo recuerde de forma automática.

Existen dos formas principales de hacerlo. **La Opción 1 es la más rápida**, y la **Opción 2 es la más segura**.

---

## Opción 1: Git Credential Store (La más rápida) ⚡

Esta opción le dice a Git que guarde tus credenciales en un archivo de texto en tu servidor la próxima vez que las escribas. 

1. En la terminal de tu servidor, ejecuta este comando:
   ```bash
   git config --global credential.helper store
   ```
2. Ahora, haz un `git pull` o `git push` normalmente. 
3. Git te pedirá tu usuario y tu **Personal Access Token (PAT)** (las contraseñas normales ya no funcionan en GitHub) por última vez.
4. **¡Listo!** Git creará un archivo oculto llamado `~/.git-credentials` y nunca más te volverá a pedir la contraseña en esa máquina.

*(⚠️ **Advertencia de seguridad:** Este método guarda tu token en texto plano en la máquina. Como son tus propios servidores privados en AWS, el riesgo es bajo, pero es importante saberlo).*

### 🤖 ¿Cómo aplicarlo a todas las instancias de una sola vez?
Dado que este método es simplemente ejecutar un comando de consola, en lugar de entrar máquina por máquina, puedes automatizarlo para todas tus instancias a la vez usando el archivo `~/.ssh/config` que configuramos antes.

Desde la terminal de tu máquina base (el Proxy o Master), crea un archivo:
```bash
nano config_git_all.sh
```

Pega este bloque de código:
```bash
#!/bin/bash
SERVIDORES=("master" "worker1" "worker2" "worker3")

echo "Configurando almacenamiento de credenciales de Git en todas las máquinas..."

for servidor in "${SERVIDORES[@]}"; do
  echo "➡️ Configurando en: $servidor"
  ssh $servidor "git config --global credential.helper store"
done

echo "✅ ¡Listo! Git recordará tus contraseñas en todos los nodos."
```

Guarda el archivo, dale permisos de ejecución (`chmod +x config_git_all.sh`) y ejecútalo (`./config_git_all.sh`). 

*(Nota: Aunque corras el script, igualmente tendrás que hacer un primer `git pull` manual en cada máquina y escribir tu contraseña una única vez para que Git la guarde en el almacenamiento local de ese nodo).*

---

## Opción 2: Autenticación por Llaves SSH (La más segura) 🔐

Este es el estándar de la industria para servidores de producción. En lugar de usar contraseñas o tokens, creas una llave criptográfica en el servidor y le dices a GitHub que confíe en esa llave.

### Paso 1: Generar la llave SSH en tu servidor
Ejecuta esto en la terminal del servidor (presiona `Enter` a todo lo que te pregunte para dejar los valores por defecto):
```bash
ssh-keygen -t ed25519 -C "tu_correo@ejemplo.com"
```

### Paso 2: Mostrar la llave pública
Ejecuta este comando para ver la llave que acabas de crear:
```bash
cat ~/.ssh/id_ed25519.pub
```
Copia todo el texto que sale en pantalla (empieza con `ssh-ed25519...`).

### Paso 3: Agregar la llave a GitHub
1. Ve a [GitHub.com](https://github.com) > **Settings** (Configuración de tu cuenta).
2. En el menú izquierdo, haz clic en **SSH and GPG keys**.
3. Haz clic en el botón verde **New SSH key**.
4. Ponle un título (ej. "Airflow Master AWS").
5. Pega la llave que copiaste en el paso 2 y guárdala.

### Paso 4: Cambiar la URL de tu repositorio a SSH
Actualmente, seguro clonaste tu proyecto usando HTTPS (ej. `https://github.com/usuario/repo.git`). Para usar la llave SSH, debes cambiar la URL del repositorio en tu servidor.

Entra a la carpeta de tu proyecto y ejecuta:
```bash
git remote set-url origin git@github.com:TU_USUARIO/TU_REPOSITORIO.git
```
*(Asegúrate de cambiar TU_USUARIO y TU_REPOSITORIO por los correctos).*

**¡Listo!** Ahora, cada vez que hagas `git pull` o `git push`, Git usará la llave SSH de la máquina de forma invisible y 100% segura, sin pedirte ninguna contraseña.
