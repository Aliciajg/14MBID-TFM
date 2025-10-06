# /app/src/spark_stream.py
# -*- coding: utf-8 -*-
import os, datetime, json
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.dataframe import DataFrame
import joblib
import numpy as np
import pandas as pd

# ========= Sesión de Spark (motor de streaming estructurado) =========
# Se inicializa una SparkSession específica para streaming. Las opciones seleccionadas
# están pensadas para un entorno de desarrollo/validación reproducible dentro de contenedores:
# - spark.sql.shuffle.partitions: se limita a 8 para evitar un número excesivo de particiones
#   en operaciones con shuffle (joins/aggregations), reduciendo coste en entornos pequeños.
# - spark.sql.adaptive.enabled: desactivado para evitar planes que cambien dinámicamente
#   durante pruebas y facilitar diagnósticos consistentes.
# - *fileoutputcommitter*: se fuerza el algoritmo v2 para mejorar rendimiento en escritura
#   a disco local y se evitan ficheros de *_SUCCESS* innecesarios, simplificando el repositorio.
spark = (
    SparkSession.builder
    .appName("tfm-riego-stream")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ========= Identificador de ejecución para checkpoints =========
# Para diferenciar ejecuciones en desarrollo se genera un RUN_ID con marca temporal.
# Si DEV_UNIQUE_CP=0, se usa "stable" para reutilizar el mismo checkpoint y facilitar
# pruebas idempotentes.
DEV_UNIQUE_CP = os.getenv("DEV_UNIQUE_CP", "1") == "1"
RUN_ID = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") if DEV_UNIQUE_CP else "stable"

# ========= Rutas de checkpoints y datos de salida =========
# Se definen ubicaciones persistentes dentro del contenedor:
# - CHECKPOINT_*: estado de las queries (necesario en Structured Streaming).
# - *_PARQUET: zonas de aterrizaje de lecturas y predicciones en formato columna (Parquet).
BASE_CP            = "/app/checkpoints"
CHECKPOINT_CONSOLE = f"{BASE_CP}/console/{RUN_ID}"
CHECKPOINT_SENSORS = f"{BASE_CP}/sensors/{RUN_ID}"
SENSORS_PARQUET    = "/app/data/sensors_parquet"
PRED_PARQUET       = "/app/data/predictions_parquet"

# Permitir reconfigurar rutas del modelo vía variables de entorno facilita reemplazar
# artefactos sin reconstruir la imagen (ej. cambios de versión del pipeline).
MODEL_PATH    = os.getenv("MODEL_PATH",    "/app/models/pipeline.joblib")
ENCODERS_PATH = os.getenv("ENCODERS_PATH", "/app/models/label_encoders.pkl")

# ========= Carga del modelo de ML y detección de su “modo” de entrada =========
# El streaming debe adaptar la preparación de variables según el tipo de artefacto:
# 1) Si el modelo es un Pipeline que incluye OneHotEncoder, se pueden pasar categorías
#    como texto crudo y el pipeline las transformará internamente.
# 2) Si el modelo es un estimador “pelado” (sin OHE), hay que transformar categóricas
#    con LabelEncoders previamente serializados.
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

def _model_expects_raw_categories(m) -> bool:
    """Devuelve True si el artefacto es un Pipeline que, en algún nivel, contiene OneHotEncoder.
       Esto nos indica que aceptará cadenas categóricas crudas y gestionará el one-hot interno."""
    try:
        if isinstance(m, Pipeline):
            # Se inspeccionan pasos directos y anidados (ColumnTransformer y subpipelines).
            for _, step in m.named_steps.items():
                if isinstance(step, OneHotEncoder):
                    return True
                if isinstance(step, ColumnTransformer):
                    for _, trf, _ in step.transformers_:
                        if isinstance(trf, OneHotEncoder):
                            return True
                        if isinstance(trf, Pipeline):
                            for __, s in trf.named_steps.items():
                                if isinstance(s, OneHotEncoder):
                                    return True
        return False
    except Exception:
        # Por robustez: ante cualquier incidencia, asumimos que no es pipeline con OHE.
        return False

try:
    model = joblib.load(MODEL_PATH)
    print(f"[INFO] Modelo cargado: {MODEL_PATH}")
except Exception as e:
    # Fallar pronto (fail-fast) para no arrancar el streaming sin capacidad de predecir.
    raise RuntimeError(f"No se pudo cargar el modelo en {MODEL_PATH}: {e}")

MODEL_WANTS_RAW = _model_expects_raw_categories(model)
print(f"[INFO] El modelo {'ES' if MODEL_WANTS_RAW else 'NO ES'} Pipeline+OneHotEncoder;"
      f" usaré {'TEXTO CRUDO' if MODEL_WANTS_RAW else 'LabelEncoders'} para categóricas.")

# ========= Carga de encoders cuando el modelo los requiere =========
# Si el artefacto NO gestiona categóricas internamente, los LabelEncoders son obligatorios
# para mapear categorías vistas a índices enteros y controlar no vistas (asignando -1).
encoders = None
if MODEL_WANTS_RAW:
    # Encoders opcionales: si existen, se cargan por compatibilidad, pero no son estrictamente necesarios.
    if os.path.exists(ENCODERS_PATH):
        try:
            encoders = joblib.load(ENCODERS_PATH)
            print(f"[INFO] Encoders cargados (opcionales): {ENCODERS_PATH}")
        except Exception as e:
            print(f"[WARN] No se pudieron cargar encoders opcionales: {e}")
else:
    # En modo estimador “pelado” los encoders son requeridos; si faltan, se interrumpe la ejecución.
    try:
        encoders = joblib.load(ENCODERS_PATH)
        print(f"[INFO] Encoders cargados (requeridos): {ENCODERS_PATH}")
    except Exception as e:
        raise RuntimeError(f"Modelo sin Pipeline: se requieren encoders en {ENCODERS_PATH}. Error: {e}")

# ========= Esquema objetivo para el JSON recibido desde Kafka =========
# Se define el contrato de datos esperado en el campo "value" del topic:
# identificadores, marca temporal y variables agroambientales necesarias para el modelo.
schema = T.StructType([
    T.StructField("deviceId",     T.StringType()),
    T.StructField("ts",           T.StringType()),
    T.StructField("cropId",       T.StringType()),
    T.StructField("soilType",     T.StringType()),
    T.StructField("stage",        T.StringType()),
    T.StructField("soilMoisture", T.DoubleType()),
    T.StructField("moistureIdx",  T.DoubleType()),
    T.StructField("tempC",        T.DoubleType()),
    T.StructField("humidity",     T.DoubleType()),
])

# ========= Fuente de datos: lector de Kafka (Structured Streaming) =========
# Se suscribe al topic "sensors" del broker interno. Se configura:
# - startingOffsets: por defecto "latest" para procesar sólo nuevos eventos (útil en validación).
# - failOnDataLoss=false: tolerancia a reinicios de procesos y rotación de segmentos.
kafka_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")
         .option("subscribe", "sensors")
         .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
         .option("failOnDataLoss", "false")
         .load()
)

# ========= Parseo de JSON y normalización de columnas =========
# 1) Se castea key/value a texto y se aplica from_json con el esquema definido.
# 2) Se “aplana” el objeto y se armonizan variantes de humedad (soilMoisture en [0,1] o
#    moistureIdx en [0,100]) a una métrica común:
#    - moisture01: proporción [0,1]
#    - moisturePct: porcentaje [0,100]
# 3) Se tipan las variables numéricas y se materializa event_time a partir de ts.
json_df = kafka_df.select(
    F.col("key").cast("string").alias("key"),
    F.from_json(F.col("value").cast("string"), schema).alias("j")
)

flat = json_df.select(
    F.coalesce(F.col("j.deviceId"), F.col("key")).alias("deviceId"),
    F.col("j.ts").alias("ts_raw"),
    F.col("j.cropId").alias("cropId"),
    F.col("j.soilType").alias("soilType"),
    F.col("j.stage").alias("stage"),
    F.col("j.soilMoisture").alias("soilMoisture"),
    F.col("j.moistureIdx").alias("moistureIdx"),
    F.col("j.tempC").alias("tempC"),
    F.col("j.humidity").alias("humidity")
)

clean = (
    flat
    .withColumn("ts", F.to_timestamp("ts_raw")).drop("ts_raw")
    .withColumn(
        "moisture01",
        F.when(F.col("soilMoisture").isNotNull(), F.col("soilMoisture"))
         .otherwise(F.when(F.col("moistureIdx").isNotNull(), F.col("moistureIdx") / F.lit(100.0)))
    )
    .withColumn("moisturePct", (F.col("moisture01") * F.lit(100.0)).cast("double"))
    .withColumn("tempC",    F.col("tempC").cast("double"))
    .withColumn("humidity", F.col("humidity").cast("double"))
    .withColumn("event_time", F.col("ts"))
)

# ========= Utilidad: comprobación robusta de micro-lotes vacíos =========
# En Structured Streaming es más fiable forzar un count() sobre un select literal limitado
# que usar df.rdd.isEmpty(). Esta utilidad se usa como “early-out” para evitar escrituras vacías.
def _is_empty(df: DataFrame) -> bool:
    return df.select(F.lit(1)).limit(1).count() == 0

# ========= Predicción por micro-lote: preparación dual según el artefacto =========
# Esta función se invoca desde foreachBatch. Su cometido:
# - Seleccionar y tipar las features requeridas por el modelo.
# - Si el modelo es Pipeline+OHE, pasar categóricas como texto (con "missing" para NaN).
# - Si el modelo es estimador “pelado”, aplicar LabelEncoders y mapear no vistas a -1.
# - Devolver un DataFrame con las predicciones anexadas como 'needsIrrigation'.
def predict_batch(df: DataFrame) -> DataFrame:
    """
    - Si el modelo es Pipeline con OneHotEncoder -> alimentar con cadenas crudas.
    - Si es estimador "pelado" -> aplicar LabelEncoders (categorías no vistas -> -1).
    Features esperadas (orden): ["MOI","temp","humidity","crop ID","soil_type","Seedling Stage"]
    """
    base = df.select(
        "deviceId","ts","cropId","soilType","stage",
        "moisturePct","tempC","humidity"
    )
    pdf = base.toPandas()
    if pdf.empty:
        return spark.createDataFrame(pdf.assign(needsIrrigation=pd.Series(dtype="int64")))

    feat = pd.DataFrame(index=pdf.index)
    # Variables numéricas principales (humedad del suelo en %; temperatura; HR)
    feat["MOI"]      = pd.to_numeric(pdf["moisturePct"], errors="coerce")
    feat["temp"]     = pd.to_numeric(pdf["tempC"],       errors="coerce")
    feat["humidity"] = pd.to_numeric(pdf["humidity"],    errors="coerce")
    seedling_mask = pdf["stage"].astype(str).str.strip().str.lower().eq("seedling")
    seedling_yn   = np.where(seedling_mask, "Yes", "No")

    if MODEL_WANTS_RAW:
        # Camino 1 — Pipeline + OHE: se entregan las categóricas como texto limpio.
        feat["crop ID"]        = pdf["cropId"].astype(str).str.strip()
        feat["soil_type"]      = pdf["soilType"].astype(str).str.strip()
        feat["Seedling Stage"] = pd.Series(seedling_yn, index=feat.index).astype(str)
        # Imputación simple para evitar NaN en OHE y asegurar dtype object.
        for c in ["crop ID","soil_type","Seedling Stage"]:
            feat[c] = feat[c].fillna("missing").astype(object)
    else:
        # Camino 2 — Estimador pelado: se aplican LabelEncoders persistidos.
        feat["crop ID"]        = pdf["cropId"].astype(str).str.strip()
        feat["soil_type"]      = pdf["soilType"].astype(str).str.strip()
        feat["Seedling Stage"] = pd.Series(seedling_yn, index=feat.index).astype(str)

        def safe_transform(le, series: pd.Series) -> np.ndarray:
            # Mapea categorías vistas a su índice; las no vistas a -1 para ser reconocibles.
            classes = list(le.classes_)
            mapping = {cls: idx for idx, cls in enumerate(classes)}
            return series.astype(str).str.strip().map(lambda v: mapping.get(v, -1)).astype(int).values

        required_encoders = ["crop ID", "soil_type", "Seedling Stage"]
        for col in required_encoders:
            if encoders is None or col not in encoders:
                raise RuntimeError(f"Encoder requerido para '{col}' no encontrado en {ENCODERS_PATH}")
            feat[col] = safe_transform(encoders[col], feat[col])

    # Se asegura el orden exacto de entrada que el modelo espera.
    expected = ["MOI","temp","humidity","crop ID","soil_type","Seedling Stage"]
    feat = feat[expected]

    # Inferencia sobre el micro-lote y reconstrucción del DataFrame de salida con etiqueta binaria.
    y_pred = model.predict(feat)

    pdf_out = pdf.assign(needsIrrigation=y_pred)
    return spark.createDataFrame(pdf_out)

# ========= Lógica por micro-lote: persistencia y trazabilidad =========
# Esta función se ejecuta para cada micro-lote que llega por la query principal:
# 1) Filtra/selecciona columnas limpias y escribe lecturas en Parquet (particionado por fecha).
# 2) Genera predicciones, las escribe en Parquet (mismo particionado) y muestra un log
#    ordenado por tiempo en consola para observabilidad rápida.
def process_batch(df: DataFrame, batch_id: int):
    # Cortocircuito si el micro-lote está vacío (no se realizan escrituras).
    if _is_empty(df):
        print(f"[INFO] Lote {batch_id}: vacío (skip writes)")
        return

    # Persistencia de lecturas normalizadas (zona “sensors”).
    df_sensors = (
        df.select("deviceId","ts","cropId","soilType","stage","moisture01","moisturePct","tempC","humidity")
          .withColumn("date", F.to_date("ts"))
    )

    if _is_empty(df_sensors):
        print(f"[INFO] Lote {batch_id}: sin filas útiles tras limpieza (skip sensors write)")
        return

    (df_sensors
        .write.mode("append").partitionBy("date").parquet(SENSORS_PARQUET)
    )

    # Inferencia del modelo y escritura de resultados (zona “predictions”).
    pred_df = predict_batch(df).withColumn("date", F.to_date("ts"))

    if _is_empty(pred_df):
        print(f"[INFO] Lote {batch_id}: sin filas tras predicción (skip preds write)")
        return

    (pred_df
        .select("deviceId","ts","cropId","stage","moisturePct","tempC","humidity","needsIrrigation","date")
        .write.mode("append").partitionBy("date").parquet(PRED_PARQUET)
    )

    # Observabilidad: muestra en consola un extracto ordenado por tiempo de evento.
    (pred_df
        .select("ts","deviceId","cropId","stage","moisturePct","tempC","humidity","needsIrrigation")
        .orderBy(F.col("ts").asc_nulls_last())
        .show(50, truncate=False)
    )

# ========= Sinks de streaming: consola (inspección) y foreachBatch (persistencia/ML) =========
# Se mantienen dos queries simultáneas:
# - console_q: imprime lecturas básicas en consola para validación rápida del flujo.
# - sink_q: ejecuta la ruta de negocio (limpieza → predicción → escritura Parquet) por micro-lote.
console_q = (
    clean
    .select("ts","deviceId","cropId","stage","moisturePct","tempC","humidity")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "50")
    .option("checkpointLocation", CHECKPOINT_CONSOLE)
    .queryName(f"tfm-riego-console-{RUN_ID}")
    .start()
)

sink_q = (
    clean
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_SENSORS)
    .outputMode("append")
    .queryName(f"tfm-riego-sink-{RUN_ID}")
    .start()
)

# ========= Bloqueo de la aplicación =========
# La aplicación queda a la espera de cualquiera de las queries activas. Este bloqueo
# permite que el proceso de streaming se mantenga en ejecución hasta parada explícita.
spark.streams.awaitAnyTermination()
