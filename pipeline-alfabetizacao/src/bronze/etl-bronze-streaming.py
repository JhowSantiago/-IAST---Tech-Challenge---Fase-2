"""
Job AWS Glue — ingestão streaming na camada Bronze.

Ponto de entrada para execução no AWS Glue. A lógica de transformação
está em load_streaming.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bronze.load_streaming import construir_bronze_streaming, salvar_bronze_streaming  # noqa: E402
from src.dq.checks import checar_qualidade, get_checks  # noqa: E402
from src.streaming.eventos import ENTIDADE_STREAMING  # noqa: E402


def main() -> None:
    args = getResolvedOptions(sys.argv, ["BUCKET_BRONZE", "SOURCE_PATH"])
    bucket_bronze = args["BUCKET_BRONZE"]
    source_path = args["SOURCE_PATH"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df = spark.read.parquet(source_path)
    df_bronze = construir_bronze_streaming(df)
    checar_qualidade(df_bronze, get_checks(ENTIDADE_STREAMING, "bronze"))

    total = df_bronze.count()
    destino = salvar_bronze_streaming(df_bronze, bucket_bronze)
    print("=" * 60)
    print("SUMÁRIO BRONZE — INGESTÃO STREAMING")
    print(f"Entidade : {ENTIDADE_STREAMING}")
    print(f"Registros: {total}")
    print(f"Destino  : {destino}")
    print("Status   : SUCESSO")
    print("=" * 60)


if __name__ == "__main__":
    main()
