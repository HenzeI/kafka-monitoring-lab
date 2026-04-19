# Kafka Monitoring Lab

Simulación de monitorización de infraestructura mediante Apache Kafka y MongoDB.  
Varios servidores (simulados por un productor) reportan métricas de rendimiento a un sistema centralizado. Un consumidor recoge esas métricas, las almacena en bruto en MongoDB y calcula KPIs agregados mediante ventanas tumbling.

---

## Arquitectura

```
┌─────────────────────┐        ┌───────────────────┐        ┌──────────────────────┐
│     PRODUCTOR       │        │       KAFKA        │        │     CONSUMIDOR       │
│                     │        │                    │        │                      │
│  web01, web02,      │──────▶│ system-metrics-    │──────▶│  Inserta en MongoDB  │
│  db01, app01,       │ JSON  │ topic              │ JSON  │  · system_metrics_raw│
│  cache01            │        │                    │        │  · system_metrics_kpis│
└─────────────────────┘        └───────────────────┘        └──────────────────────┘
```

---

## Estructura del proyecto

```
kafka-monitoring-lab/
│
├── producer/
│   └── productor_metrics.py     # Genera y envía métricas a Kafka
│
├── consumer/
│   ├── consumidor_metrics.py    # Consume Kafka e inserta en MongoDB
│   ├── test_consumidor.py       # Script de prueba: lee el topic sin MongoDB
│   └── test_mongodb.py          # Script de prueba: valida la conexión a MongoDB
│
├── .env                         # Variables de entorno (no subir a git)
├── .env.example                 # Plantilla de variables de entorno
├── docker-compose.yml           # Zookeeper + Kafka en local
├── evidencias.md                # Capturas de pantalla del laboratorio
├── requirements.txt             # Dependencias Python
└── README.md
```

---

## Formato del mensaje Kafka

Cada mensaje publicado en el topic tiene la siguiente estructura JSON:

```json
{
  "server_id": "web01",
  "timestamp_utc": "2026-04-19T10:23:45.123456+00:00",
  "metrics": {
    "cpu_percent": 47.32,
    "memory_percent": 61.80,
    "disk_io_mbps": 23.10,
    "network_mbps": 55.40,
    "error_count": 0
  },
  "message_uuid": "a1b2c3d4-..."
}
```

---

## Colecciones MongoDB

### `system_metrics_raw`
Cada mensaje recibido se almacena íntegramente con metadatos de Kafka:

| Campo              | Descripción                        |
|--------------------|------------------------------------|
| `server_id`        | Identificador del servidor         |
| `timestamp_utc`    | Marca temporal del mensaje         |
| `metrics`          | Objeto con las 5 métricas          |
| `message_uuid`     | UUID único del mensaje             |
| `_kafka_offset`    | Offset en la partición Kafka       |
| `_kafka_partition` | Partición Kafka del mensaje        |

### `system_metrics_kpis`
Se genera un documento cada 20 mensajes (ventana tumbling):

| Campo                      | Descripción                              |
|----------------------------|------------------------------------------|
| `timestamp`                | Momento de cierre de la ventana          |
| `window_duration_seconds`  | Duración real de la ventana en segundos  |
| `message_count`            | Número de mensajes procesados (20)       |
| `kpis.avg_cpu_percent`     | Promedio de CPU                          |
| `kpis.avg_memory_percent`  | Promedio de memoria                      |
| `kpis.avg_disk_io_mbps`    | Promedio de disco                        |
| `kpis.avg_network_mbps`    | Promedio de red                          |
| `kpis.total_error_count`   | Suma total de errores                    |
| `kpis.processing_rate_msg_per_sec` | Tasa de procesamiento (msg/s)  |

---

## Quickstart

### 1. Requisitos previos

- Docker Desktop en ejecución
- Python 3.11 (recomendado) con el entorno virtual activado
- Cuenta en MongoDB Atlas con el clúster configurado y tu IP en la lista blanca

### 2. Levantar Kafka

```bash
docker-compose up -d
```

Verifica que los contenedores estén activos:

```bash
docker ps
```

### 3. Configurar variables de entorno

Copia el ejemplo y rellena tus credenciales:

```bash
cp .env.example .env
```

Edita `.env`:

```env
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_TOPIC=system-metrics-topic

MONGO_URI=mongodb+srv://<usuario>:<contraseña>@<cluster>.mongodb.net/?retryWrites=true&w=majority
MONGO_DB_NAME=nombre_base_de_datos
MONGO_RAW_COLLECTION=system_metrics_raw
MONGO_KPI_COLLECTION=system_metrics_kpis

INTERVALO_SEGUNDOS=10
WINDOW_SIZE=20
```

### 4. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 5. (Opcional) Verificar conexión a MongoDB

```bash
python consumer/test_mongodb.py
```

### 6. (Opcional) Verificar lectura del topic Kafka

Ejecuta primero el productor (paso 7) y luego en otra terminal:

```bash
python consumer/test_consumidor.py
```

### 7. Ejecutar el productor

```bash
python producer/productor_metrics.py
```

Genera métricas para los 5 servidores cada 10 segundos y las publica en Kafka.

### 8. Ejecutar el consumidor

En otra terminal:

```bash
python consumer/consumidor_metrics.py
```

Consume los mensajes de Kafka, los guarda en `system_metrics_raw` y genera un documento KPI en `system_metrics_kpis` cada 20 mensajes.

---

## Dependencias

```
kafka-python
pymongo
python-dotenv
```
---
 *Autor: Hancel Fernando*