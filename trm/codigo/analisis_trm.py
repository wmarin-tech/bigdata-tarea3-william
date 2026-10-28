"""Analisis de la TRM con Spark. Tarea 3 - Grupo 115.
Corte: 27 de octubre de 2026.
Promedios calculados por dia calendario de vigencia.
"""
from functools import reduce
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TRM_Grupo115").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ruta = "hdfs://localhost:9000/user/hadoop/tarea3/trm/trm_historico.csv"
salida = Path("/home/hadoop/tarea3_trm/resultados_automaticos")
salida.mkdir(parents=True, exist_ok=True)
corte = F.lit("2026-10-27").cast("date")

original = spark.read.option("header", True).csv(ruta)
trm = original.select(
    F.col("VALOR").cast("decimal(12,2)").alias("trm"),
    F.trim("UNIDAD").alias("unidad"),
    F.to_date("VIGENCIADESDE", "dd/MM/yyyy").alias("desde"),
    F.to_date("VIGENCIAHASTA", "dd/MM/yyyy").alias("hasta")
).cache()

# Validaciones: detener el analisis si se detectan problemas.
nulos = reduce(lambda a, b: a | b,
               [F.col(c).isNull() for c in trm.columns])
invalidos = trm.filter(
    nulos | (F.col("trm") <= 0) |
    (F.col("unidad") != "COP") |
    (F.col("hasta") < F.col("desde"))
).count()
total = trm.count()
duplicados = total - trm.dropDuplicates().count()
posteriores = trm.filter(F.col("desde") > corte).count()

print("Registros originales:", total)
print("Registros invalidos:", invalidos)
print("Duplicados exactos:", duplicados)
print("Registros posteriores al corte:", posteriores)

if invalidos or duplicados:
    raise ValueError("Revisar la calidad de los datos antes de continuar.")

# Expandir cada intervalo de vigencia a dias calendario.
diario = trm.filter(F.col("desde") <= corte).select(
    F.explode(
        F.sequence("desde", F.least(F.col("hasta"), corte))
    ).alias("fecha"),
    "trm"
).cache()

control = diario.agg(
    F.count("*").alias("filas"),
    F.countDistinct("fecha").alias("unicos"),
    F.min("fecha").alias("inicio"),
    F.max("fecha").alias("fin")
).first()

if control["filas"] == 0:
    raise ValueError("No hay datos para el corte seleccionado.")

esperados = (control["fin"] - control["inicio"]).days + 1
if not control["filas"] == control["unicos"] == esperados:
    raise ValueError("Existen fechas repetidas o dias faltantes.")

print("Dias verificados:", esperados)
print("Periodo:", control["inicio"], "a", control["fin"])

def estadisticas():
    return [
        F.count("*").alias("dias"),
        F.min("trm").alias("minimo"),
        F.max("trm").alias("maximo"),
        F.round(F.avg("trm"), 2).alias("promedio")
    ]

resumen = diario.agg(*estadisticas())

anual = diario.groupBy(F.year("fecha").alias("anio")).agg(
    *estadisticas()
)
anual = anual.withColumn(
    "cobertura",
    F.when(
        F.col("dias") == F.dayofyear(
            F.make_date(F.col("anio"), F.lit(12), F.lit(31))
        ), "Completo"
    ).otherwise("Parcial")
)

mensual = diario.groupBy(
    F.trunc("fecha", "month").alias("mes")
).agg(*estadisticas())
mensual = mensual.withColumn(
    "cobertura",
    F.when(
        F.col("dias") == F.dayofmonth(F.last_day("mes")),
        "Completo"
    ).otherwise("Parcial")
)

extremos = resumen.first()
fechas_extremas = diario.filter(
    (F.col("trm") == extremos["minimo"]) |
    (F.col("trm") == extremos["maximo"])
).orderBy("fecha")

print("RESUMEN GENERAL")
resumen.show()
print("FECHAS DE LOS VALORES EXTREMOS")
fechas_extremas.show()
print("RESUMEN ANUAL")
anual.orderBy("anio").show(40, truncate=False)
print("MESES COMPLETOS CON MAYOR PROMEDIO")
mensual.filter("cobertura = 'Completo'").orderBy(
    F.desc("promedio"), "mes"
).show(5, truncate=False)
print("MESES COMPLETOS CON MENOR PROMEDIO")
mensual.filter("cobertura = 'Completo'").orderBy(
    "promedio", "mes"
).show(5, truncate=False)

tablas = {
    "serie_diaria": diario.orderBy("fecha"),
    "resumen_general": resumen,
    "resumen_anual": anual.orderBy("anio"),
    "resumen_mensual": mensual.orderBy("mes"),
    "fechas_extremas": fechas_extremas
}
for nombre, tabla in tablas.items():
    tabla.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv((salida / nombre).as_uri())

print("ANALISIS TERMINADO. Resultados en:", salida)
spark.stop()
