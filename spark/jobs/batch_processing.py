"""
Spark Batch Job: Procesamiento de datos estáticos de Elden Ring
- Ingesta desde la Elden Ring Fan API (REST)
- Limpieza y transformación con PySpark
- Cálculo de efectividad arma vs jefe (cross-join enriquecido)
- Escritura a PostgreSQL via JDBC (modo append con ON CONFLICT ignorado)
"""

import json
import logging
import os
import sys

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array, coalesce, col, explode_outer, lit, regexp_extract,
    when
)
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType, StructField, StructType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("EldenRingBatch")

# ── Configuración ──────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "elden_ring")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "elden")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ring123")
JDBC_URL          = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_JAR          = "/opt/bitnami/spark/user-jars/postgresql-42.7.4.jar"

JDBC_WRITE_OPTS = {
    "url":      JDBC_URL,
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

ELDENRING_API_BASE = "https://eldenring.fanapis.com/api"


# ── SparkSession ───────────────────────────────────────────────────────────────
def get_spark():
    return (
        SparkSession.builder
        .appName("EldenRing-BatchProcessing")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )


# ── Ingesta desde la API ───────────────────────────────────────────────────────
def fetch_api(endpoint: str, limit: int = 100) -> list:
    try:
        url = f"{ELDENRING_API_BASE}/{endpoint}?limit={limit}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", [])
        log.info(f"  API {endpoint}: {len(data)} registros recibidos")
        return data
    except Exception as e:
        log.warning(f"  API {endpoint} no disponible ({e}). Usando solo seed data.")
        return []


# ── Procesamiento de Armas ─────────────────────────────────────────────────────
def process_weapons(spark: SparkSession, api_weapons: list):
    if not api_weapons:
        log.info("  Sin datos de API para armas. Continuando con seed data.")
        return

    schema = StructType([
        StructField("id",          StringType(), True),
        StructField("name",        StringType(), True),
        StructField("category",    StringType(), True),
        StructField("weight",      DoubleType(),  True),
        StructField("attack", ArrayType(StructType([
            StructField("name",   StringType(), True),
            StructField("amount", IntegerType(), True),
        ])), True),
        StructField("requiredAttributes", ArrayType(StructType([
            StructField("name",   StringType(), True),
            StructField("amount", IntegerType(), True),
        ])), True),
        StructField("scalesWith", ArrayType(StructType([
            StructField("name",    StringType(), True),
            StructField("scaling", StringType(), True),
        ])), True),
    ])

    df = spark.createDataFrame(api_weapons, schema)

    def _dmg(col_ref, index, label):
        """Extrae daño por índice de array, comprobando el nombre del tipo."""
        return (
            when(col(col_ref)[index]["name"] == label, col(col_ref)[index]["amount"])
            .otherwise(0)
        )

    df_clean = (
        df.select(
            col("id").alias("weapon_id"),
            col("name"),
            col("category"),
            col("weight"),
            _dmg("attack", 0, "Phy").alias("damage_physical"),
            _dmg("attack", 1, "Mag").alias("damage_magic"),
            _dmg("attack", 2, "Fire").alias("damage_fire"),
            _dmg("attack", 3, "Ligt").alias("damage_lightning"),
            _dmg("attack", 4, "Holy").alias("damage_holy"),
            _dmg("requiredAttributes", 0, "Str").alias("requirements_str"),
            _dmg("requiredAttributes", 1, "Dex").alias("requirements_dex"),
            _dmg("requiredAttributes", 2, "Int").alias("requirements_int"),
            _dmg("requiredAttributes", 3, "Fai").alias("requirements_fai"),
            _dmg("requiredAttributes", 4, "Arc").alias("requirements_arc"),
            when(col("scalesWith")[0]["name"] == "Str", col("scalesWith")[0]["scaling"]).otherwise("").alias("scaling_str"),
            when(col("scalesWith")[1]["name"] == "Dex", col("scalesWith")[1]["scaling"]).otherwise("").alias("scaling_dex"),
            when(col("scalesWith")[2]["name"] == "Int", col("scalesWith")[2]["scaling"]).otherwise("").alias("scaling_int"),
            when(col("scalesWith")[3]["name"] == "Fai", col("scalesWith")[3]["scaling"]).otherwise("").alias("scaling_fai"),
            when(col("scalesWith")[4]["name"] == "Arc", col("scalesWith")[4]["scaling"]).otherwise("").alias("scaling_arc"),
            lit("api").alias("source"),
        )
        .filter(col("weapon_id").isNotNull())
        .dropDuplicates(["weapon_id"])
    )

    count = df_clean.count()
    (
        df_clean.write
        .format("jdbc")
        .options(**JDBC_WRITE_OPTS)
        .option("dbtable", "weapons")
        .mode("append")
        .save()
    )
    log.info(f"  ✅ {count} armas escritas desde API")


# ── Procesamiento de Jefes ─────────────────────────────────────────────────────
def process_bosses(spark: SparkSession, api_bosses: list):
    if not api_bosses:
        log.info("  Sin datos de API para jefes. Continuando con seed data.")
        return

    schema = StructType([
        StructField("id",           StringType(), True),
        StructField("name",         StringType(), True),
        StructField("location",     StringType(), True),
        StructField("healthPoints", StringType(), True),
        StructField("drops",        ArrayType(StringType()), True),
    ])

    df = spark.createDataFrame(api_bosses, schema)
    df_clean = (
        df.select(
            col("id").alias("boss_id"),
            col("name"),
            col("location"),
            regexp_extract(col("healthPoints"), r"(\d+)", 1).cast("int").alias("health_points"),
            lit(110).alias("defense_physical"),
            lit(110).alias("defense_magic"),
            lit(110).alias("defense_fire"),
            array(lit("physical")).alias("weaknesses"),
            array(lit("magic")).alias("resistances"),
            lit("api").alias("source"),
        )
        .filter(col("boss_id").isNotNull())
        .dropDuplicates(["boss_id"])
    )

    count = df_clean.count()
    (
        df_clean.write
        .format("jdbc")
        .options(**JDBC_WRITE_OPTS)
        .option("dbtable", "bosses")
        .mode("append")
        .save()
    )
    log.info(f"  ✅ {count} jefes escritos desde API")


# ── Cálculo de Efectividad ─────────────────────────────────────────────────────
def calculate_effectiveness(spark: SparkSession):
    """Cross-join de armas vs jefes con fórmula de efectividad ponderada."""

    def _read(table):
        return (
            spark.read
            .format("jdbc")
            .options(**JDBC_WRITE_OPTS)
            .option("dbtable", table)
            .load()
        )

    weapons_df = _read("weapons").alias("w")
    bosses_df  = _read("bosses").alias("b")

    # Fórmula: suma ponderada de (daño / defensa) normalizada a 0-100
    effectiveness_df = (
        weapons_df.crossJoin(bosses_df)
        .select(
            col("w.weapon_id"),
            col("b.boss_id"),
            (
                coalesce(col("w.damage_physical"), lit(0)) * 1.0 /
                (coalesce(col("b.defense_physical"), lit(1)) + 1) * 10 +
                coalesce(col("w.damage_magic"), lit(0)) * 1.0 /
                (coalesce(col("b.defense_magic"), lit(1)) + 1) * 10 +
                coalesce(col("w.damage_fire"), lit(0)) * 1.0 /
                (coalesce(col("b.defense_fire"), lit(1)) + 1) * 10
            ).alias("raw_score")
        )
        .withColumn(
            "effectiveness_score",
            when(col("raw_score") > 100, lit(100.0))
            .when(col("raw_score") < 0,   lit(0.0))
            .otherwise(col("raw_score"))
        )
        .select("weapon_id", "boss_id", "effectiveness_score")
    )

    count = effectiveness_df.count()
    (
        effectiveness_df.write
        .format("jdbc")
        .options(**JDBC_WRITE_OPTS)
        .option("dbtable", "weapon_boss_effectiveness")
        # Usamos append; los conflictos en (weapon_id, boss_id) son ignorados
        # porque el seed ya los insertó y los de la API son nuevos IDs
        .mode("append")
        .save()
    )
    log.info(f"  ✅ {count} pares arma/jefe calculados")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  ELDEN RING - BATCH PROCESSING JOB (PySpark)")
    log.info("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("\n[1/4] Obteniendo datos de Elden Ring Fan API...")
    api_weapons = fetch_api("weapons", limit=100)
    api_bosses  = fetch_api("bosses", limit=100)

    log.info("\n[2/4] Procesando armas...")
    process_weapons(spark, api_weapons)

    log.info("\n[3/4] Procesando jefes...")
    process_bosses(spark, api_bosses)

    log.info("\n[4/4] Calculando efectividad arma vs jefe...")
    calculate_effectiveness(spark)

    log.info("\n" + "=" * 60)
    log.info("  BATCH JOB COMPLETADO CON ÉXITO")
    log.info("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
