"""
Job AWS Glue — ingestão batch na camada Bronze.

Ponto de entrada para execução no AWS Glue. A lógica de transformação
está em load_batch.py, reutilizada pelo script local de carga.
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.glue_bootstrap import setup_glue_path

setup_glue_path()

from src.bronze.load_batch import construir_bronze, imprimir_sumario, salvar_bronze  # noqa: E402
from src.dq.checks import checar_qualidade, get_checks  # noqa: E402


def main() -> None:
    args = getResolvedOptions(sys.argv, ["ENTIDADE", "BUCKET_BRONZE", "SOURCE_PATH"])
    entidade = args["ENTIDADE"]
    bucket_bronze = args["BUCKET_BRONZE"]
    source_path = args["SOURCE_PATH"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df = glue_context.spark_session.read.parquet(source_path)
    df_bronze = construir_bronze(df, entidade)
    checar_qualidade(df_bronze, get_checks(entidade, "bronze"))

    total = df_bronze.count()
    destino = salvar_bronze(df_bronze, bucket_bronze, entidade)
    imprimir_sumario(entidade, total, destino)


if __name__ == "__main__":
    main()
