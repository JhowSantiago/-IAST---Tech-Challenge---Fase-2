"""Persistência da camada Silver no S3 (PySpark)."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def adicionar_silver_timestamp(df: DataFrame) -> DataFrame:
    agora = datetime.now(timezone.utc)
    return df.withColumn("_silver_processed_at", F.lit(agora))


def salvar_silver(df: DataFrame, bucket_silver: str, entidade: str) -> str:
    destino = f"s3://{bucket_silver}/silver/{entidade}/"
    (
        df.withColumn("mes", F.lpad(F.month(F.current_timestamp()).cast("string"), 2, "0"))
        .withColumn("dia", F.lpad(F.dayofmonth(F.current_timestamp()).cast("string"), 2, "0"))
        .write.mode("overwrite")
        .partitionBy("ano", "mes", "dia")
        .parquet(destino)
    )
    return destino


def salvar_quarentena(df: DataFrame, bucket_silver: str, entidade: str) -> str:
    destino = f"s3://{bucket_silver}/quarentena/{entidade}/"
    (
        df.withColumn("mes", F.lpad(F.month(F.current_timestamp()).cast("string"), 2, "0"))
        .withColumn("dia", F.lpad(F.dayofmonth(F.current_timestamp()).cast("string"), 2, "0"))
        .write.mode("overwrite")
        .partitionBy("ano", "mes", "dia")
        .parquet(destino)
    )
    return destino
