"""
PySpark batch transformation:
- Reads raw CSVs from data/raw/ (local)
- Drops Spanish duplicate columns and metadata columns
- Renames to clean snake_case
- Extracts year, quarter and period_date from TIME_PERIOD_CODE
- Writes Parquet to data/processed/ (local), partitioned by year
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

load_dotenv()

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("canary-islands-tourism-transform")
        .master("local[*]")
        .getOrCreate()
    )


def drop_spanish_and_metadata_cols(df: DataFrame) -> DataFrame:
    """Drop columns ending in #es and known metadata columns."""
    metadata_cols = {
        "CONFIDENCIALIDAD_OBSERVACION#en",
        "CONFIDENCIALIDAD_OBSERVACION#es",
        "NOTAS_OBSERVACION",
        "ESTADO_OBSERVACION#en",
        "ESTADO_OBSERVACION#es",
        "ESTADO_OBSERVACION_CODE",
    }
    to_drop = [c for c in df.columns if c.endswith("#es") or c in metadata_cols]
    return df.drop(*to_drop)


def rename_english_cols(df: DataFrame) -> DataFrame:
    """Remove '#en' suffix from column names."""
    rename_map = {c: c.replace("#en", "") for c in df.columns if "#en" in c}
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df


def to_snake_case(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def normalize_column_names(df: DataFrame) -> DataFrame:
    for col in df.columns:
        df = df.withColumnRenamed(col, to_snake_case(col))
    return df


def add_time_columns(df: DataFrame) -> DataFrame:
    """Extract year, quarter and period_date from TIME_PERIOD_CODE (e.g. '2018-Q1').
    period_date maps each quarter to the first day of its first month:
      Q1 -> YYYY-01-01, Q2 -> YYYY-04-01, Q3 -> YYYY-07-01, Q4 -> YYYY-10-01
    """
    df = df.withColumn("year", F.split(F.col("time_period_code"), "-Q").getItem(0).cast("int"))
    df = df.withColumn("quarter", F.split(F.col("time_period_code"), "-Q").getItem(1).cast("int"))
    quarter_to_month = F.create_map(
        F.lit(1), F.lit("01"),
        F.lit(2), F.lit("04"),
        F.lit(3), F.lit("07"),
        F.lit(4), F.lit("10"),
    )
    df = df.withColumn(
        "period_date",
        F.to_date(
            F.concat(F.col("year").cast("string"), F.lit("-"), quarter_to_month[F.col("quarter")], F.lit("-01")),
            "yyyy-MM-dd",
        ),
    )
    return df


def filter_empty_obs(df: DataFrame) -> DataFrame:
    """Remove rows where OBS_VALUE is null or empty."""
    return df.filter(F.col("obs_value").isNotNull() & (F.col("obs_value") != ""))


def transform_accommodations(spark: SparkSession) -> None:
    print("Transforming tourist_accommodations ...")
    df = spark.read.option("header", True).option("sep", ",").csv(
        f"{RAW_DIR}/tourist_accommodations.csv"
    )
    df = drop_spanish_and_metadata_cols(df)
    df = rename_english_cols(df)
    df = normalize_column_names(df)
    df = add_time_columns(df)
    df = filter_empty_obs(df)
    df = df.withColumn("obs_value", F.col("obs_value").cast("long"))

    out = f"{PROCESSED_DIR}/tourist_accommodations"
    df.write.mode("overwrite").partitionBy("year").parquet(out)
    print(f"  -> Written to {out}/ (partitioned by year)")


def transform_age_sex(spark: SparkSession) -> None:
    print("Transforming tourist_age_sex ...")
    df = spark.read.option("header", True).option("sep", ",").csv(
        f"{RAW_DIR}/tourist_age_sex.csv"
    )
    df = drop_spanish_and_metadata_cols(df)
    df = rename_english_cols(df)
    df = normalize_column_names(df)
    df = add_time_columns(df)
    df = filter_empty_obs(df)
    df = df.withColumn("obs_value", F.col("obs_value").cast("long"))

    out = f"{PROCESSED_DIR}/tourist_age_sex"
    df.write.mode("overwrite").partitionBy("year").parquet(out)
    print(f"  -> Written to {out}/ (partitioned by year)")


def transform_revenue(spark: SparkSession) -> None:
    print("Transforming tourist_revenue ...")
    df = spark.read.option("header", True).option("sep", ",").csv(
        f"{RAW_DIR}/tourist_revenue.csv"
    )
    df = drop_spanish_and_metadata_cols(df)
    df = rename_english_cols(df)
    df = normalize_column_names(df)
    df = add_time_columns(df)
    df = filter_empty_obs(df)
    df = df.withColumn("obs_value", F.col("obs_value").cast("long"))

    out = f"{PROCESSED_DIR}/tourist_revenue"
    df.write.mode("overwrite").partitionBy("year").parquet(out)
    print(f"  -> Written to {out}/ (partitioned by year)")


def run() -> None:
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    transform_accommodations(spark)
    transform_age_sex(spark)
    transform_revenue(spark)

    spark.stop()
    print("Spark transformation complete.")


if __name__ == "__main__":
    run()
