import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from datetime import datetime, timezone

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "kafka_monitoring")
RAW_COLLECTION = os.getenv("MONGO_RAW_COLLECTION", "system_metrics_raw")
KPI_COLLECTION = os.getenv("MONGO_KPI_COLLECTION", "system_metrics_kpis")


def test_conexion(client: MongoClient) -> bool:
    print("1) Probando conexión al servidor...")
    client.admin.command("ping")
    print("   OK - Conexión establecida con MongoDB Atlas")
    return True


def test_insertar(db) -> str:
    print("2) Probando inserción en system_metrics_raw...")
    doc = {
        "server_id": "test_server",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "cpu_percent": 42.0,
            "memory_percent": 55.0,
            "disk_io_mbps": 10.5,
            "network_mbps": 100.0,
            "error_count": 0,
        },
        "_test": True,
    }
    result = db[RAW_COLLECTION].insert_one(doc)
    print(f"   OK - Documento insertado con _id: {result.inserted_id}")
    return result.inserted_id


def test_leer(db, inserted_id) -> None:
    print("3) Probando lectura del documento insertado...")
    doc = db[RAW_COLLECTION].find_one({"_id": inserted_id})
    if doc:
        print(f"   OK - Documento encontrado: server_id='{doc['server_id']}' | cpu={doc['metrics']['cpu_percent']}%")
    else:
        raise AssertionError("Documento no encontrado tras la inserción")


def test_eliminar(db, inserted_id) -> None:
    print("4) Limpiando documento de prueba...")
    db[RAW_COLLECTION].delete_one({"_id": inserted_id})
    print("   OK - Documento de prueba eliminado")


def test_listar_colecciones(db) -> None:
    print("5) Colecciones existentes en la base de datos...")
    colecciones = db.list_collection_names()
    if colecciones:
        for col in colecciones:
            print(f"   - {col}")
    else:
        print("   (ninguna colección creada aún)")


def main():
    print("=" * 50)
    print(f"Test de conexión MongoDB Atlas")
    print(f"Base de datos : {DB_NAME}")
    print("=" * 50)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]

        test_conexion(client)
        inserted_id = test_insertar(db)
        test_leer(db, inserted_id)
        test_eliminar(db, inserted_id)
        test_listar_colecciones(db)

        print("=" * 50)
        print("Todos los tests pasaron correctamente")

    except ServerSelectionTimeoutError as e:
        print(f"\n[ERROR] No se pudo conectar a MongoDB Atlas (timeout):\n  {e}")
    except ConnectionFailure as e:
        print(f"\n[ERROR] Fallo de conexión:\n  {e}")
    except Exception as e:
        print(f"\n[ERROR] Error inesperado:\n  {type(e).__name__}: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
