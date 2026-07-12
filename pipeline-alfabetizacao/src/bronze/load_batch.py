"""Funções compartilhadas de ingestão batch na camada Bronze."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

ENTIDADES_TERRITORIAIS = {"uf", "municipio"}
JOB_NAME = "etl-bronze-batch"


def construir_bronze(df: DataFrame, entidade: str) -> DataFrame:
    agora = datetime.now(timezone.utc)
    data_str = agora.strftime("%Y-%m-%d")

    colunas_hash = [c for c in df.columns if not c.startswith("_")]
    expressao_hash = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in colunas_hash]
    )
    df = (
        df.withColumn("_ingestion_timestamp", F.lit(agora).cast(TimestampType()))
        .withColumn("_ingestion_date", F.lit(data_str))
        .withColumn("_source_entity", F.lit(entidade))
        .withColumn("_job_name", F.lit(JOB_NAME))
        .withColumn("_record_hash", F.md5(expressao_hash))
    )

    if entidade in ENTIDADES_TERRITORIAIS:
        df = (
            df.withColumn("ano", F.lit(str(agora.year)))
            .withColumn("mes", F.lit(f"{agora.month:02d}"))
            .withColumn("dia", F.lit(f"{agora.day:02d}"))
        )
    else:
        df = (
            df.withColumn("mes", F.lit(f"{agora.month:02d}"))
            .withColumn("dia", F.lit(f"{agora.day:02d}"))
        )

    return df


def salvar_bronze(df: DataFrame, bucket_bronze: str, entidade: str) -> str:
    destino = f"s3a://{bucket_bronze}/bronze/batch/{entidade}/"
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("ano", "mes", "dia")
        .parquet(destino)
    )
    return destino.replace("s3a://", "s3://")


def imprimir_sumario(entidade: str, total: int, destino: str) -> None:
    print("=" * 60)
    print("SUMÁRIO BRONZE — INGESTÃO BATCH")
    print(f"Entidade : {entidade}")
    print(f"Registros: {total}")
    print(f"Destino  : {destino}")
    print("Status   : SUCESSO")
    print("=" * 60)
