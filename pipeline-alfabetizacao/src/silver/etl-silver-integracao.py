"""Job AWS Glue — integração Silver (metas + território + streaming)."""

from __future__ import annotations

import sys
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.silver.load_silver import adicionar_silver_timestamp, salvar_quarentena, salvar_silver  # noqa: E402

ENTIDADE_INTEGRADA = "municipio_indicador_completo"


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["BUCKET_BRONZE", "BUCKET_SILVER", "SILVER_PREFIX"],
    )
    bucket_bronze = args["BUCKET_BRONZE"]
    bucket_silver = args["BUCKET_SILVER"]
    prefix = args.get("SILVER_PREFIX", f"s3://{bucket_silver}/silver/")

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    meta_municipio = spark.read.parquet(f"{prefix}meta_municipio/")
    municipio = spark.read.parquet(f"{prefix}municipio/")
    meta_uf = spark.read.parquet(f"{prefix}meta_uf/")

    integrado = (
        meta_municipio.join(municipio.select("id_municipio", "nome_municipio", "nome_uf"), on="id_municipio", how="left")
        .join(
            meta_uf.select("sigla_uf", "ano", F.col("taxa_alfabetizacao").alias("indicador_uf")),
            on=["sigla_uf", "ano"],
            how="left",
        )
        .withColumn("_source_type", F.lit("batch"))
    )

    orfaos = integrado.filter(F.col("nome_municipio").isNull())
    validos = integrado.filter(F.col("nome_municipio").isNotNull())

    try:
        streaming = spark.read.parquet(f"s3://{bucket_bronze}/bronze/streaming/indicador_alfabetizacao/")
        streaming = (
            streaming.withColumn("rede", F.lit("total"))
            .withColumn("_source_type", F.lit("streaming"))
            .dropDuplicates(["event_id"])
        )
        combinado = validos.unionByName(streaming, allowMissingColumns=True)
    except Exception:
        combinado = validos

    combinado = adicionar_silver_timestamp(combinado)
    destino = salvar_silver(combinado, bucket_silver, ENTIDADE_INTEGRADA)
    if orfaos.count() > 0:
        salvar_quarentena(orfaos.withColumn("_motivo_quarentena", F.lit("municipio_inexistente")), bucket_silver, ENTIDADE_INTEGRADA)
    print(f"Integração Silver: {combinado.count()} registros em {destino}")


if __name__ == "__main__":
    main()
