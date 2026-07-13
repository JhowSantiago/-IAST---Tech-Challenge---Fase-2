"""Job AWS Glue — Silver → Gold."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from src.glue_bootstrap import setup_glue_path

setup_glue_path()

from src.gold.agregacoes import AGREGACOES, VISOES_GOLD  # noqa: E402


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["VISAO", "BUCKET_SILVER", "BUCKET_GOLD", "SILVER_PATH"],
    )
    visao = args["VISAO"]
    bucket_gold = args["BUCKET_GOLD"]
    silver_path = args["SILVER_PATH"]

    if visao not in AGREGACOES:
        raise ValueError(f"VISAO inválida: {visao}. Opções: {VISOES_GOLD}")

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    df = spark.read.parquet(silver_path)
    gold_df = AGREGACOES[visao](df).withColumn("_gold_processed_at", F.lit(datetime.now(timezone.utc)))

    destino = f"s3://{bucket_gold}/gold/{visao}/"
    (
        gold_df.withColumn("mes", F.lpad(F.month(F.current_timestamp()).cast("string"), 2, "0"))
        .withColumn("dia", F.lpad(F.dayofmonth(F.current_timestamp()).cast("string"), 2, "0"))
        .write.mode("overwrite")
        .partitionBy("mes", "dia")
        .parquet(destino)
    )
    print(f"Gold {visao}: {gold_df.count()} registros em {destino}")


if __name__ == "__main__":
    main()
