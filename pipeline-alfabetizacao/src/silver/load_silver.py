"""Persistência da camada Silver no S3 (PySpark)."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ENTIDADES_PARTICAO_ANO = {"uf", "municipio"}


def adicionar_silver_timestamp(df: DataFrame) -> DataFrame:
    agora = datetime.now(timezone.utc)
    return df.withColumn("_silver_processed_at", F.lit(agora))


def _particoes(entidade: str) -> list[str]:
    if entidade in ENTIDADES_PARTICAO_ANO:
        return ["ano", "mes", "dia"]
    return ["mes", "dia"]


def _gravar_particionado(df: DataFrame, destino: str, entidade: str) -> str:
    part_cols = _particoes(entidade)
    escrita = df.withColumn("mes", F.lpad(F.month(F.current_timestamp()).cast("string"), 2, "0")).withColumn(
        "dia", F.lpad(F.dayofmonth(F.current_timestamp()).cast("string"), 2, "0")
    )
    if "ano" in part_cols and "ano" not in escrita.columns:
        escrita = escrita.withColumn("ano", F.year(F.current_timestamp()).cast("string"))
    (
        escrita.write.mode("overwrite")
        .partitionBy(*part_cols)
        .parquet(destino)
    )
    return destino


def salvar_silver(df: DataFrame, bucket_silver: str, entidade: str) -> str:
    destino = f"s3://{bucket_silver}/silver/{entidade}/"
    return _gravar_particionado(df, destino, entidade)


def salvar_quarentena(df: DataFrame, bucket_silver: str, entidade: str) -> str:
    destino = f"s3://{bucket_silver}/quarentena/{entidade}/"
    return _gravar_particionado(df, destino, entidade)
