import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# --- Configuración ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "system-metrics-topic")
SERVERS = ["web01", "web02", "db01", "app01", "cache01"]
INTERVALO_SEGUNDOS = int(os.getenv("INTERVALO_SEGUNDOS", 10))  # tiempo entre reportes completos de todos los servidores


def generar_metricas(server_id: str) -> dict:
    cpu_percent = random.uniform(5.0, 75.0)
    # Pico ocasional de CPU (10% de probabilidad)
    if random.random() < 0.1:
        cpu_percent = random.uniform(85.0, 98.0)

    memory_percent = random.uniform(20.0, 85.0)
    # Pico ocasional de memoria (5% de probabilidad)
    if random.random() < 0.05:
        memory_percent = random.uniform(90.0, 99.0)

    disk_io_mbps = random.uniform(0.1, 50.0)
    network_mbps = random.uniform(1.0, 100.0)

    # Errores poco frecuentes (8% de probabilidad)
    error_count = 0
    if random.random() < 0.08:
        error_count = random.randint(1, 3)

    return {
        "server_id": server_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "cpu_percent": round(cpu_percent, 2),
            "memory_percent": round(memory_percent, 2),
            "disk_io_mbps": round(disk_io_mbps, 2),
            "network_mbps": round(network_mbps, 2),
            "error_count": error_count,
        },
        "message_uuid": str(uuid.uuid4()),
    }


def on_send_error(exc):
    print(f"[ERROR] Fallo al enviar mensaje: {exc}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print("Iniciando simulación de generación de métricas...")
    print(f"Servidores simulados: {SERVERS}")
    print(f"Intervalo de reporte: {INTERVALO_SEGUNDOS} segundos")
    print("-" * 30)

    try:
        while True:
            print(f"\n{datetime.now()}: Generando reporte de métricas...")

            # Iterar sobre cada servidor para generar y enviar sus métricas
            for server_id in SERVERS:
                mensaje = generar_metricas(server_id)

                producer.send(TOPIC, value=mensaje).add_errback(on_send_error)

                print(f"  Enviado para {server_id}:")
                print(f"    CPU:    {mensaje['metrics']['cpu_percent']}%")
                print(f"    Mem:    {mensaje['metrics']['memory_percent']}%")
                print(f"    Disco:  {mensaje['metrics']['disk_io_mbps']} MB/s")
                print(f"    Red:    {mensaje['metrics']['network_mbps']} Mbps")
                print(f"    Errores:{mensaje['metrics']['error_count']}")

            producer.flush()
            print(f"\nReporte completo enviado. Esperando {INTERVALO_SEGUNDOS} segundos...")
            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
