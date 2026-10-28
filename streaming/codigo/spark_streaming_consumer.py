"""Analisis de sensores con Spark Structured Streaming y Kafka."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, round
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType
)

spark = (
    SparkSession.builder
    .appName("Sensores_William_Grupo115")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "America/Bogota")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType()),
])

mensajes = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "earliest")
    .load()
)

datos = (
    mensajes
    .select(
        from_json(col("value").cast("string"), schema).alias("dato")
    )
    .select("dato.*")
    .withColumn("fecha_hora", col("timestamp").cast("timestamp"))
)

resumen = (
    datos
    .groupBy(window(col("fecha_hora"), "1 minute"), "sensor_id")
    .agg(
        count("*").alias("lecturas"),
        round(avg("temperature"), 2).alias("temperatura_promedio"),
        round(avg("humidity"), 2).alias("humedad_promedio"),
    )
    .select(
        col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        "sensor_id",
        "lecturas",
        "temperatura_promedio",
        "humedad_promedio",
    )
    .orderBy(col("inicio").desc(), col("sensor_id"))
)

consulta = (
    resumen.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "30")
    .option(
        "checkpointLocation",
        "file:///home/hadoop/tarea3_streaming/checkpoints/sensores",
    )
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    consulta.awaitTermination()
except KeyboardInterrupt:
    pass
finally:
    consulta.stop()
    spark.stop()
