import os, json
from confluent_kafka import Producer

# === Configuración del productor Kafka ===
# Se obtiene la URL del broker desde variable de entorno (permite correr en local o en Docker
# sin cambiar código). Si no está definida, se usa localhost:29092 por defecto.
bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

# Se instancia el productor de Kafka con el bootstrap configurado.
# Este objeto gestiona el buffer de mensajes y la conexión al broker.
p = Producer({"bootstrap.servers": bootstrap})

# === Mensajes de ejemplo (simulación de telemetría IoT) ===
# Cada elemento es una tupla (key, value):
# - key: identificador del dispositivo (bytes en Kafka), útil para particionado ordenado.
# - value: payload JSON con el esquema que espera Spark en el consumidor.
#
# Notas de esquema:
# - 'ts' está en ISO-8601 (UTC) para orden temporal.
# - Humedad puede venir como:
#   - soilMoisture en [0,1]  (proporción)
#   - moistureIdx  en [0,100] (porcentaje)
#   Spark armoniza ambas a moisturePct en el flujo downstream.
msgs = [
  ("dev-001", {"deviceId":"dev-001","ts":"2025-09-09T16:00:01Z",
               "cropId":"wheat","soilType":"loam","stage":"Vegetative",
               "soilMoisture":0.42,"tempC":22.1,"humidity":55}),
  ("dev-002", {"deviceId":"dev-002","ts":"2025-09-09T16:00:05Z",
               "cropId":"Potato","soilType":"Sandy Soil","stage":"Flowering",
               "moistureIdx":54,"tempC":24.0,"humidity":80}),
  ("dev-003", {"deviceId":"dev-003","ts":"2025-09-09T16:00:05Z",
               "cropId":"Wheat","soilType":"Black Soil","stage":"Germination",
               "moistureIdx":1,"tempC":25.0,"humidity":80}),
]

# === Envío de eventos al topic 'sensors' ===
# Para cada mensaje:
# - Se serializa el value a JSON (bytes) y la key a bytes.
# - p.produce() coloca el mensaje en el buffer de salida.
# - p.poll(0) bombea el loop interno del cliente para procesar callbacks y I/O.
for k, v in msgs:
    p.produce("sensors", key=k.encode(), value=json.dumps(v).encode())
    p.poll(0)

# flush() bloquea hasta que todo el buffer se haya entregado al broker (o venza el timeout).
# Garantiza que los mensajes queden efectivamente publicados antes de finalizar el proceso.
p.flush()

print("enviados:", len(msgs))
