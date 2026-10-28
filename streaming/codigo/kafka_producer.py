"""Generador de sensores simulados basado en el Anexo 3."""

import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8"),
    acks="all",
)

print("Enviando datos a sensor_data. Para detener: Ctrl+C.", flush=True)

try:
    while True:
        dato = {
            "sensor_id": random.randint(1, 10),
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 70), 2),
            "timestamp": int(time.time()),
        }

        confirmacion = producer.send("sensor_data", value=dato).get(timeout=10)

        print(
            f"Enviado: {json.dumps(dato)} "
            f"| particion={confirmacion.partition} "
            f"| offset={confirmacion.offset}",
            flush=True,
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.", flush=True)

finally:
    producer.flush(timeout=10)
    producer.close(timeout=10)
