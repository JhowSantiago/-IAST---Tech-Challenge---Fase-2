"""Agregações PySpark da camada Gold (AWS Glue)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.gold.agregacoes_pandas import (
    VISAO_EVOLUCAO_TEMPORAL,
    VISAO_INDICADOR_MUNICIPIO,
    VISAO_META_VS_RESULTADO,
    VISOES_GOLD,
)

META_COLS = [f"meta_alfabetizacao_{y}" for y in range(2024, 2031)]


def _com_meta_vigente(df: DataFrame) -> DataFrame:
    meta_por_ano = F.coalesce(
        *[
            F.when(F.col("ano") == y, F.col(f"meta_alfabetizacao_{y}")).otherwise(F.lit(None))
            for y in range(2024, 2031)
        ]
    )
    meta = F.coalesce(meta_por_ano, F.col("meta_alfabetizacao_2030"))
    return (
        df.withColumn("meta_vigente", meta)
        .withColumn("gap_meta", F.round(F.col("taxa_alfabetizacao") - F.col("meta_vigente"), 2))
        .withColumn(
            "atingiu_meta",
            F.when(
                F.col("taxa_alfabetizacao").isNotNull() & F.col("meta_vigente").isNotNull(),
                F.col("taxa_alfabetizacao") >= F.col("meta_vigente"),
            ),
        )
    )


def agregar_indicador_municipio(df: DataFrame) -> DataFrame:
    base = _com_meta_vigente(df.filter(F.col("_source_type") == "batch"))
    return base.select(
        F.col("id_municipio"),
        F.col("nome_municipio").alias("nome"),
        F.col("sigla_uf"),
        F.col("nome_uf"),
        F.col("ano"),
        F.col("rede"),
        F.col("taxa_alfabetizacao").alias("pct_alfabetizados"),
        F.col("meta_vigente").alias("meta_pct"),
        F.col("gap_meta"),
        F.col("atingiu_meta"),
        F.col("indicador_uf"),
    )


def agregar_meta_vs_resultado(df: DataFrame) -> DataFrame:
    base = _com_meta_vigente(df.filter(F.col("_source_type") == "batch"))
    por_uf = base.groupBy("sigla_uf", "ano").agg(
        F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        F.round(F.avg("meta_vigente"), 2).alias("meta_media"),
        F.round(F.avg("gap_meta"), 2).alias("gap_medio"),
        F.countDistinct("id_municipio").alias("municipios_total"),
        F.sum(F.when(F.col("atingiu_meta"), 1).otherwise(0)).alias("municipios_acima_meta"),
        F.sum(F.when(~F.col("atingiu_meta"), 1).otherwise(0)).alias("municipios_abaixo_meta"),
    )
    w = Window.partitionBy("sigla_uf").orderBy("ano")
    return por_uf.withColumn("taxa_anterior", F.lag("taxa_media").over(w)).withColumn(
        "delta_percentual", F.round(F.col("taxa_media") - F.col("taxa_anterior"), 2)
    )


def agregar_evolucao_temporal(df: DataFrame) -> DataFrame:
    base = _com_meta_vigente(df.filter(F.col("_source_type") == "batch"))
    w = Window.partitionBy("id_municipio", "rede").orderBy("ano")
    return (
        base.withColumnRenamed("nome_municipio", "nome")
        .withColumnRenamed("taxa_alfabetizacao", "pct_alfabetizados")
        .withColumnRenamed("meta_vigente", "meta_pct")
        .withColumn("pct_anterior", F.lag("pct_alfabetizados").over(w))
        .withColumn("delta_percentual", F.round(F.col("pct_alfabetizados") - F.col("pct_anterior"), 2))
        .withColumn("delta_anual", F.col("delta_percentual"))
    )


AGREGACOES = {
    VISAO_INDICADOR_MUNICIPIO: agregar_indicador_municipio,
    VISAO_META_VS_RESULTADO: agregar_meta_vs_resultado,
    VISAO_EVOLUCAO_TEMPORAL: agregar_evolucao_temporal,
}

__all__ = ["AGREGACOES", "VISOES_GOLD"]
