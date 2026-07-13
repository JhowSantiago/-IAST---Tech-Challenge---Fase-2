"""Job AWS Glue — Bronze → Silver."""

from __future__ import annotations

import sys
from pathlib import Path

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.dq.checks import checar_qualidade, get_checks  # noqa: E402
from src.silver.load_silver import adicionar_silver_timestamp, salvar_silver  # noqa: E402
from src.silver.transformacoes import TRANSFORMACOES  # noqa: E402


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["ENTIDADE", "BUCKET_BRONZE", "BUCKET_SILVER", "BRONZE_PATH"],
    )
    entidade = args["ENTIDADE"]
    bucket_silver = args["BUCKET_SILVER"]
    bronze_path = args["BRONZE_PATH"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df = spark.read.parquet(bronze_path)
    transformar = TRANSFORMACOES[entidade]
    if entidade == "meta_municipio":
        municipios_path = bronze_path.replace(entidade, "municipio")
        municipios = spark.read.parquet(municipios_path)
        df_silver = transformar(df, municipios)
    else:
        df_silver = transformar(df)

    df_silver = adicionar_silver_timestamp(df_silver)
    checar_qualidade(df_silver, get_checks(entidade, "silver"))
    destino = salvar_silver(df_silver, bucket_silver, entidade)
    print(f"Silver {entidade}: {df_silver.count()} registros em {destino}")


if __name__ == "__main__":
    main()
