import json
import os
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "system-metrics-topic")
GROUP_ID = f"metrics-consumer-group-{uuid.uuid4()}"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "kafka_monitoring")
RAW_COLLECTION = os.getenv("MONGO_RAW_COLLECTION", "system_metrics_raw")
KPI_COLLECTION = os.getenv("MONGO_KPI_COLLECTION", "system_metrics_kpis")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", 20))  # ventana tumbling en número de mensajes


def calculate_kpis(window: list, window_start: float) -> dict:
    duration = time.time() - window_start
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "window_duration_seconds": round(duration, 3),
        "message_count": len(window),
        "kpis": {
            "avg_cpu_percent": round(sum(m["metrics"]["cpu_percent"] for m in window) / len(window), 4),
            "avg_memory_percent": round(sum(m["metrics"]["memory_percent"] for m in window) / len(window), 4),
            "avg_disk_io_mbps": round(sum(m["metrics"]["disk_io_mbps"] for m in window) / len(window), 4),
            "avg_network_mbps": round(sum(m["metrics"]["network_mbps"] for m in window) / len(window), 4),
            "total_error_count": sum(m["metrics"]["error_count"] for m in window),
            "processing_rate_msg_per_sec": round(len(window) / duration, 4) if duration > 0 else 0,
        },
    }


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    mongo = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    db = mongo[DB_NAME]
    raw_col = db[RAW_COLLECTION]
    kpi_col = db[KPI_COLLECTION]

    print(f"Consumidor iniciado | group_id={GROUP_ID}")
    print(f"Almacenando raw -> {RAW_COLLECTION} | KPIs (cada {WINDOW_SIZE} msgs) -> {KPI_COLLECTION}")

    window: list = []
    window_start = time.time()

    try:
        for msg in consumer:
            record = msg.value

            # Insertar documento en bruto
            raw_col.insert_one({**record, "_kafka_offset": msg.offset, "_kafka_partition": msg.partition})
            print(f"[RAW] {record.get('server_id')} | offset={msg.offset} | uuid={record.get('message_uuid', '')[:8]}")

            window.append(record)

            if len(window) >= WINDOW_SIZE:
                kpi_doc = calculate_kpis(window, window_start)
                kpi_col.insert_one(kpi_doc)
                print(
                    f"[KPI] ventana cerrada | duración={kpi_doc['window_duration_seconds']}s | "
                    f"avg_cpu={kpi_doc['kpis']['avg_cpu_percent']}% | "
                    f"rate={kpi_doc['kpis']['processing_rate_msg_per_sec']} msg/s | "
                    f"errors={kpi_doc['kpis']['total_error_count']}"
                )
                window = []
                window_start = time.time()

    except KeyboardInterrupt:
        print("\nConsumidor detenido.")
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
