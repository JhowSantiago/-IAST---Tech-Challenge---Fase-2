"""Funções compartilhadas de ingestão streaming na camada Bronze."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from src.streaming.eventos import ENTIDADE_STREAMING, JOB_NAME_STREAMING

COLUNAS_EVENTO = [
    "event_id",
    "event_type",
    "timestamp",
    "id_municipio",
    "sigla_uf",
    "ano",
    "taxa_alfabetizacao",
    "meta",
]


def construir_bronze_streaming(df: DataFrame) -> DataFrame:
    agora = datetime.now(timezone.utc)
    data_str = agora.strftime("%Y-%m-%d")

    colunas_hash = [c for c in df.columns if not c.startswith("_")]
    expressao_hash = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in colunas_hash]
    )
    return (
        df.withColumn("_ingestion_timestamp", F.lit(agora).cast(TimestampType()))
        .withColumn("_ingestion_date", F.lit(data_str))
        .withColumn("_source_entity", F.lit(ENTIDADE_STREAMING))
        .withColumn("_source_type", F.lit("streaming"))
        .withColumn("_job_name", F.lit(JOB_NAME_STREAMING))
        .withColumn("_record_hash", F.md5(expressao_hash))
        .withColumn("mes", F.lit(f"{agora.month:02d}"))
        .withColumn("dia", F.lit(f"{agora.day:02d}"))
    )


def salvar_bronze_streaming(df: DataFrame, bucket_bronze: str) -> str:
    destino = f"s3a://{bucket_bronze}/bronze/streaming/{ENTIDADE_STREAMING}/"
    (
        df.write.mode("append")
        .option("compression", "snappy")
        .partitionBy("ano", "mes", "dia")
        .parquet(destino)
    )
    return destino.replace("s3a://", "s3://")
