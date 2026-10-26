# bigdata-cluster
# Práctica 1: Clúster Big Data (HDFS y Spark)

## Arquitectura de Red (Vagrant)
- **Master:** 192.168.56.10 (NameNode + Spark Master)[cite: 1]
- **Worker1:** 192.168.56.11 (DataNode + Spark Worker)[cite: 1]
- **Worker2:** 192.168.56.12 (DataNode + Spark Worker)[cite: 1]

## Instrucciones de Pruebas Funcionales

### 1. Interfaces Web
- **HDFS NameNode:** http://192.168.56.10:50070[cite: 1]
- **Spark Master:** http://192.168.56.10:8080[cite: 1]

### 2. Subida de fichero a HDFS (Replicación 2)
```bash
docker exec -it namenode bash
hdfs dfs -D dfs.replication=2 -put archivo.txt /archivo.txt
