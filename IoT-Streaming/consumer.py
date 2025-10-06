import os, sys, json, signal
from confluent_kafka import Consumer

# === Configuración básica del cliente Kafka ===
# Se toma la URL del broker de la variable de entorno para facilitar ejecución
# en local/Docker sin tocar el código. Por defecto: localhost:29092.
bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

# Parámetros del consumidor:
# - bootstrap.servers: ubicación del broker
# - group.id: identificador del grupo de consumo (permite coordinar offsets)
# - auto.offset.reset: qué hacer si no hay offset previo (aquí, leer desde el inicio)
conf = {
    "bootstrap.servers": bootstrap,
    "group.id": "tfm-riego-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
running = True

# === Manejo elegante de parada (Ctrl+C / SIGTERM) ===
# Se captura la señal para terminar el bucle principal de forma controlada,
# cerrando el consumidor y liberando recursos.
def shutdown(*_):
    global running
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# === Suscripción al topic ===
# El consumidor se suscribe al topic 'sensors', el mismo que publica el productor
# de pruebas y del que lee Spark en el pipeline principal.
consumer.subscribe(["sensors"])
print("Escuchando en 'sensors'… (Ctrl+C para salir)")

try:
    # === Bucle de consumo ===
    # Se hace polling con timeout de 1s. Si no hay mensaje, se itera de nuevo.
    # Si llega un error de librería/broker, se reporta por stderr y se continúa.
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}", file=sys.stderr)
            continue

        # Decodificación de key y value a texto (UTF-8). El productor envía
        # la key como bytes del deviceId y el value como JSON serializado.
        key = msg.key().decode() if msg.key() else None
        value = msg.value().decode() if msg.value() else None

        # Para inspección manual: imprime "deviceId:payload_json".
        # Útil para verificar que el broker recibe eventos con el esquema esperado.
        print(f"{key}:{value}")
finally:
    # === Cierre ordenado ===
    # Se cierra el consumidor (commitea offsets si aplica y libera conexiones).
    consumer.close()
    print("Consumer cerrado.")
