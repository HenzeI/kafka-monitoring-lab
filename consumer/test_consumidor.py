import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "system-metrics-topic")
# group_id fijo y único para no afectar los offsets del consumidor real
GROUP_ID = "test-inspector-group"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",   # leer desde el principio del topic
        enable_auto_commit=False,        # no mover offsets, solo inspeccionar
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,        # para si no llegan mensajes en 5s
    )

    print(f"Conectado a '{TOPIC}'. Leyendo mensajes (Ctrl+C para salir)...")
    print("=" * 60)

    total = 0
    try:
        for msg in consumer:
            total += 1
            data = msg.value
            print(f"\n[MSG #{total}]  partition={msg.partition}  offset={msg.offset}")
            print(f"  server_id   : {data.get('server_id')}")
            print(f"  timestamp   : {data.get('timestamp_utc')}")
            print(f"  uuid        : {data.get('message_uuid')}")
            metricas = data.get("metrics", {})
            print(f"  cpu         : {metricas.get('cpu_percent')}%")
            print(f"  memoria     : {metricas.get('memory_percent')}%")
            print(f"  disco       : {metricas.get('disk_io_mbps')} MB/s")
            print(f"  red         : {metricas.get('network_mbps')} Mbps")
            print(f"  errores     : {metricas.get('error_count')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\n{'=' * 60}")
        print(f"Total mensajes leídos: {total}")


if __name__ == "__main__":
    main()
