# Configuración de Conexiones SSH (ProxyJump)

Esta guía explica cómo configurar tu archivo `~/.ssh/config` para establecer alias de conexión y utilizar saltos SSH (ProxyJump). Esto te permitirá conectarte a las instancias privadas (Workers) de forma directa, pasando automáticamente a través del servidor público (Master o Proxy) sin tener que recordar IPs ni ejecutar comandos largos.

## 1. Editar `~/.ssh/config` con nano

El archivo `~/.ssh/config` permite definir alias para cada conexión SSH. En lugar de escribir el comando completo con IP, usuario y llave cada vez que quieres conectarte a un Worker, defines cada entrada una sola vez y luego usas simplemente el comando `ssh worker1`.

### Abrir o crear el archivo
Desde la terminal del Master (o desde tu computadora local):

```bash
nano ~/.ssh/config
```
*(Si el archivo no existe, nano lo crea automáticamente).*

### Contenido del archivo
Copia y pega el siguiente bloque, **reemplazando las IPs con las reales de tu equipo** y verificando la ruta correcta de tus llaves `.pem`:

```ssh-config
Host master
    HostName 10.0.1.XX
    User ubuntu
    IdentityFile /home/ubuntu/master-key.pem

Host rabbitmq
    HostName 10.0.2.XX
    User ubuntu
    IdentityFile /home/ubuntu/rabbitmq-key.pem
    ProxyJump master

Host worker1
    HostName 10.0.2.XX
    User ubuntu
    IdentityFile /home/ubuntu/worker1-key.pem
    ProxyJump master

Host worker2
    HostName 10.0.2.XX
    User ubuntu
    IdentityFile /home/ubuntu/worker2-key.pem
    ProxyJump master

Host worker3
    HostName 10.0.2.XX
    User ubuntu
    IdentityFile /home/ubuntu/worker3-key.pem
    ProxyJump master
```

### Explicación línea por línea

| Directiva | Descripción |
| :--- | :--- |
| **Host** | El alias con el que invocarás la conexión, por ejemplo `ssh worker1`. |
| **HostName** | La IP privada real de la instancia. |
| **User** | El usuario del sistema operativo. En instancias Ubuntu de AWS siempre es `ubuntu`. |
| **IdentityFile** | Ruta completa al archivo `.pem` que SSH usará para autenticarse. |
| **ProxyJump** | Indica que SSH debe pasar primero por el host señalado antes de llegar al destino final. |

> [!NOTE]
> El bloque `Host master` existe para que los bloques de los Workers puedan referenciar a `master` en **ProxyJump**. Sin él, SSH no sabría cómo llegar al Master cuando intenta hacer el salto.

### Guardar y salir de nano
Una vez escrito el contenido:
1. Presiona `Ctrl + O` → escribir los cambios al archivo.
2. Presiona `Enter` → confirmar el nombre del archivo.
3. Presiona `Ctrl + X` → salir de nano.

---

## 2. Ajustar permisos del config y los `.pem`

SSH en Linux rechaza las conexiones cuando las llaves privadas o el archivo config tienen permisos demasiado abiertos. Este paso es obligatorio; sin él, los comandos del siguiente paso fallarán con una advertencia como esta:

```text
WARNING: UNPROTECTED PRIVATE KEY FILE!
Permissions 0644 for 'worker1-key.pem' are too open.
```

### Ajustar permisos de los `.pem`
```bash
chmod 400 rabbitmq-key.pem
chmod 400 worker1-key.pem
chmod 400 worker2-key.pem
chmod 400 worker3-key.pem
```

### Ajustar permisos del config
```bash
chmod 600 ~/.ssh/config
```

### Verificar que los permisos quedaron correctos
```bash
ls -la *.pem ~/.ssh/config
```
La salida debe verse así:
```text
-r-------- 1 ubuntu ubuntu 1674 Jan  1 10:00 rabbitmq-key.pem
-r-------- 1 ubuntu ubuntu 1674 Jan  1 10:00 worker1-key.pem
-r-------- 1 ubuntu ubuntu 1674 Jan  1 10:00 worker2-key.pem
-r-------- 1 ubuntu ubuntu 1674 Jan  1 10:00 worker3-key.pem
-rw------- 1 ubuntu ubuntu  312 Jan  1 10:00 /home/ubuntu/.ssh/config
```
* `-r--------` corresponde a 400. 
* `-rw-------` corresponde a 600.

---

## 3. Probar la conexión hacia cada Worker

Con el archivo config guardado y los permisos correctos, ejecuta los siguientes comandos desde tu terminal.

### Conectarse a Worker 1
```bash
ssh worker1
```

Si todo está bien configurado, SSH se encargará de hacer el puente por detrás y verás el prompt de bienvenida de Ubuntu dentro del Worker:

```text
Welcome to Ubuntu 24.04.X LTS...

ubuntu@ip-10-0-2-XX:~$
```

Para cerrar la sesión del Worker y volver a la terminal anterior, simplemente escribe:
```bash
exit
```
