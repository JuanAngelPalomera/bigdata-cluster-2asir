# Práctica 1: Infraestructura Big Data virtualizada (HDFS + Spark)

**IES Gregorio Prieto · CFGS ASIR 2.º · Virtualización y Computación en la Nube**

- **Autores:** Juan Ángel Palomera y Héctor (apellidos)
- **Repositorio:** https://github.com/JuanAngelPalomera/bigdata-cluster-2asir

Clúster Big Data distribuido con HDFS y Spark (modo standalone) desplegado con tres capas:

| Capa | Herramienta | Responsabilidad |
|---|---|---|
| Infraestructura | **Vagrant** (VirtualBox) | Crea las 3 VMs y la red privada |
| Configuración | **Ansible** (`ansible_local`) | Prepara cada VM, instala Docker y despliega los contenedores |
| Servicios | **Docker** / Docker Compose | Ejecuta NameNode, DataNodes, Spark Master y Spark Workers |

## 1. Arquitectura

| VM | IP | Contenedores |
|---|---|---|
| `master` | 192.168.56.10 | NameNode, Spark Master |
| `worker1` | 192.168.56.11 | DataNode, Spark Worker |
| `worker2` | 192.168.56.12 | DataNode, Spark Worker |

- Red privada host-only `192.168.56.0/24`.
- Factor de replicación HDFS: **2** (`dfs.replication=2`).
- Todo el software de HDFS y Spark corre en contenedores Docker.

### Interfaces web

| Servicio | URL |
|---|---|
| NameNode (HDFS) | http://192.168.56.10:50070 |
| Spark Master | http://192.168.56.10:8080 |
| DataNode worker1 | http://192.168.56.11:50075 |
| DataNode worker2 | http://192.168.56.12:50075 |
| Spark Worker worker1 | http://192.168.56.11:8081 |
| Spark Worker worker2 | http://192.168.56.12:8081 |

## 2. Estructura del proyecto

```
.
├── Vagrantfile
├── README.md
└── ansible/
    ├── inventory.ini
    ├── requirements.yml
    ├── site.yml
    └── roles/
        ├── common/        # DNS, paquetes y /etc/hosts
        ├── docker/        # Instalación de Docker Engine
        └── hdfs_spark/    # docker-compose y despliegue de HDFS/Spark
```

## 3. Requisitos del equipo anfitrión

- VirtualBox y Vagrant instalados (probado en Windows con VirtualBox 7.2).
- Virtualización activada en la BIOS (VT-x / AMD-V).
- Unos 6 GB de RAM libres y conexión a internet.
- Un adaptador host-only de VirtualBox en el rango `192.168.56.0/24` (el de por defecto). No hace falta instalar Ansible: Vagrant lo instala dentro de las VMs.

## 4. Despliegue

```bash
git clone https://github.com/JuanAngelPalomera/bigdata-cluster-2asir.git
cd bigdata-cluster-2asir
vagrant up
```

El primer arranque tarda varios minutos (descarga de la box, de paquetes y de imágenes Docker). Para destruir y repetir desde cero:

```bash
vagrant destroy -f
vagrant up
```

Estado de las máquinas: `vagrant status`. Acceso: `vagrant ssh master`.

## 5. Comprobaciones

Todos los comandos se ejecutan desde la carpeta del proyecto en el equipo anfitrión.

### 5.1 Infraestructura y contenedores

```bash
vagrant status
vagrant ssh master  -c "sudo docker ps"
vagrant ssh worker1 -c "sudo docker ps"
vagrant ssh worker2 -c "sudo docker ps"
```

Resultado esperado: en `master` los contenedores `namenode` y `spark-master`; en cada worker `datanode` y `spark-worker`.

Resolución de nombres y conectividad:

```bash
vagrant ssh master -c "getent hosts master"
vagrant ssh master -c "ping -c 2 worker1"
vagrant ssh master -c "ping -c 2 worker2"
```

### 5.2 Interfaces web (requisito funcional 1)

Abrir en el navegador del anfitrión:

1. **NameNode** `http://192.168.56.10:50070`: en *Datanodes* deben aparecer **2 DataNodes** activos.
2. **Spark Master** `http://192.168.56.10:8080`: en *Workers* deben aparecer **2 workers** en estado `ALIVE`.

Comprobación por consola del número de DataNodes vivos:

```bash
vagrant ssh master -c "sudo docker exec namenode hdfs dfsadmin -report"
```

Debe indicar `Live datanodes (2)`.

### 5.3 Subir un fichero a HDFS con replicación 2 (requisito funcional 2)

```bash
# 1. Crear un fichero de prueba en el master
vagrant ssh master -c "seq 1 200000 | sed 's/^/linea de prueba numero /' > /tmp/datos.txt"

# 2. Copiarlo al contenedor del NameNode
vagrant ssh master -c "sudo docker cp /tmp/datos.txt namenode:/tmp/datos.txt"

# 3. Crear un directorio en HDFS y subir el fichero
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -mkdir -p /datos"
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -put -f /tmp/datos.txt /datos/datos.txt"

# 4. Listar: la segunda columna del listado es el factor de replicación
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -ls /datos"

# 5. Ver bloques y en qué DataNodes están
vagrant ssh master -c "sudo docker exec namenode hdfs fsck /datos/datos.txt -files -blocks -locations"
```

Resultado esperado:

- En el `-ls`, la columna de replicación vale **2**.
- En `fsck`: `Default replication factor: 2`, `Average block replication: 2.0`, y cada bloque lista **dos** ubicaciones (las IPs de worker1 y worker2). Estado final `HEALTHY`.

### 5.4 Ejecutar un trabajo Spark (requisito funcional 3)

Wordcount en Spark que lee de HDFS y escribe el resultado en HDFS. Se lanza contra el Spark Master standalone y se fuerzan varias particiones para que el trabajo se reparta entre los dos workers:

```bash
vagrant ssh master -c "sudo docker exec -i spark-master /spark/bin/spark-shell --master spark://192.168.56.10:7077 --total-executor-cores 2 --executor-memory 512m" <<'EOF'
val texto = sc.textFile("hdfs://192.168.56.10:9000/datos/datos.txt", 8)
val conteo = texto.flatMap(_.split(" ")).map(p => (p, 1)).reduceByKey(_ + _)
conteo.saveAsTextFile("hdfs://192.168.56.10:9000/resultado")
:quit
EOF
```

**Comprobar que el resultado está en HDFS:**

```bash
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -ls /resultado"
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -cat /resultado/part-00000 | head"
```

Debe existir el fichero `_SUCCESS` y varios `part-0000X`.

**Comprobar que el trabajo se dividió entre los workers:**

Mientras el trabajo está en ejecución (o con la aplicación aún listada en *Completed Applications*), abrir `http://192.168.56.10:8080`:

1. En *Workers*, ambos muestran cores y memoria en uso.
2. Entrar en la aplicación (`Spark shell`) y abrir sus *Executors*: debe haber **un executor en 192.168.56.11 y otro en 192.168.56.12**, ambos con tareas completadas (*Completed Tasks* > 0).

Por consola, también se ve en los logs de cada worker:

```bash
vagrant ssh worker1 -c "sudo docker logs spark-worker 2>&1 | grep -i executor | tail -5"
vagrant ssh worker2 -c "sudo docker logs spark-worker 2>&1 | grep -i executor | tail -5"
```

### 5.5 Tolerancia a fallos: parar un worker (requisito funcional 4)

1. Parar el DataNode de `worker1`:

```bash
vagrant ssh worker1 -c "sudo docker stop datanode"
```

2. Leer el fichero de nuevo. Debe seguir disponible porque el bloque tiene otra réplica en `worker2`:

```bash
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -cat /datos/datos.txt | head -3"
vagrant ssh master -c "sudo docker exec namenode hdfs dfs -cat /datos/datos.txt | wc -l"
```

El número de líneas debe ser `200000`, igual que antes de parar el worker.

3. Comprobar el estado (el NameNode tarda unos minutos en marcar el DataNode como muerto; por defecto en torno a 10 minutos):

```bash
vagrant ssh master -c "sudo docker exec namenode hdfs dfsadmin -report"
vagrant ssh master -c "sudo docker exec namenode hdfs fsck /datos/datos.txt -files -blocks -locations"
```

4. Recuperar el worker:

```bash
vagrant ssh worker1 -c "sudo docker start datanode"
```

El DataNode vuelve a registrarse en el NameNode y el clúster recupera los 2 nodos.

## 6. Decisiones de diseño

- **`ansible_local` en lugar de Ansible desde Windows:** Ansible no funciona de forma nativa en Windows. Vagrant instala Ansible dentro de cada VM, de modo que `vagrant up` es lo único necesario.
- **Cada VM se configura a sí misma** (`ansible_connection=local` y `limit` por nodo): evita configurar claves SSH entre nodos.
- **Provisioner `shell` de Vagrant (excepción justificada):** VirtualBox 7.2 anuncia un DNS por IPv6 que no responde, y Vagrant necesita internet para instalar Ansible antes de que se ejecute ningún rol. Este arreglo es previo a Ansible, que aún no existe en la VM. La configuración de DNS se repite en el rol `common` para que quede documentada de forma idempotente.
- **Sin módulo `shell` de Ansible:** se usan `apt`, `file`, `copy`, `get_url`, `apt_repository`, `service`, `user`, `lineinfile`, `blockinfile`, `template` y `community.docker`.
- **`/etc/hosts` gestionado por Ansible** y eliminación del hostname de las IPs de loopback (`127.0.x.x`): si el nombre del nodo apunta a loopback, el NameNode escucha solo en local y los DataNodes no pueden conectarse.
- **Docker desde el repositorio oficial** de Docker, con su clave GPG verificada.
- **Imágenes Docker `bde2020`** (Hadoop 2.7.4 y Spark 2.4.0): imágenes preparadas para trabajar juntas, configurables por variables de entorno.
- **IPs fijas** para los tres nodos: HDFS y Spark se configuran con la dirección del master.
- **Replicación 2:** cada bloque se guarda en los dos DataNodes, por lo que la caída de un worker no hace perder datos.

## 7. Problemas conocidos y solución

| Problema | Causa | Solución |
|---|---|---|
| `Temporary failure in name resolution` al instalar Ansible | DNS IPv6 de VirtualBox 7.2 | Provisioner `shell` previo con DNS IPv4 |
| `Timed out while waiting for the machine to boot` | Puerto serie de la box y arranque lento con 3 VMs | `--uartmode1 disconnected` y `boot_timeout = 600` |
| `Failed to create the host-only adapter` | Adaptador host-only fuera del rango 192.168.56.0/24 | Reconfigurarlo con `VBoxManage hostonlyif ipconfig ... --ip 192.168.56.1` |
| `VERR_ALREADY_EXISTS` al hacer `vagrant up` tras `destroy` | Carpeta residual en `VirtualBox VMs` | Eliminar la VM con `VBoxManage unregistervm --delete` y borrar la carpeta |
| Aviso `group and host with same name` | Grupo `master` y host `master` | El grupo se llama `masters` |

## 8. Control de versiones

Repositorio Git de la pareja con commits de ambos miembros: https://github.com/JuanAngelPalomera/bigdata-cluster-2asir