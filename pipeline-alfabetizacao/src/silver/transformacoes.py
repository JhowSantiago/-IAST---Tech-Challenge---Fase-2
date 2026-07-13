"""Transformações PySpark da camada Silver (AWS Glue)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PONTO_CORTE_ALFABETIZACAO = 743.0


def transformar_uf(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla"))))
        .withColumn("nome_uf", F.initcap(F.trim(F.col("nome"))))
        .dropDuplicates(["sigla_uf"])
        .select("id_uf", "sigla_uf", "nome_uf", "regiao")
    )


def transformar_municipio(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("nome_municipio", F.initcap(F.trim(F.col("nome"))))
        .dropDuplicates(["id_municipio"])
        .select("id_municipio", "sigla_uf", "nome_municipio", "nome_uf")
    )


def _meta_vigente_expr(ano_col: str = "ano") -> F.Column:
    cols = [F.col(f"meta_alfabetizacao_{y}") for y in range(2024, 2031)]
    return F.coalesce(*cols)


def transformar_meta_municipio(df: DataFrame, municipios: DataFrame | None = None) -> DataFrame:
    resultado = df.withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
    if municipios is not None and "sigla_uf" not in resultado.columns:
        lookup = municipios.select("id_municipio", F.col("sigla_uf").alias("sigla_lookup"))
        resultado = resultado.join(lookup, on="id_municipio", how="left")
        resultado = resultado.withColumn("sigla_uf", F.coalesce(F.col("sigla_uf"), F.col("sigla_lookup"))).drop(
            "sigla_lookup"
        )
    meta = _meta_vigente_expr()
    resultado = (
        resultado.withColumn("meta_vigente", meta)
        .withColumn("gap_meta", F.col("taxa_alfabetizacao") - F.col("meta_vigente"))
        .withColumn("atingiu_meta", F.col("taxa_alfabetizacao") >= F.col("meta_vigente"))
        .drop("meta_vigente")
        .dropDuplicates(["id_municipio", "ano", "rede"])
    )
    if "nivel_alfabetizacao" in resultado.columns:
        resultado = resultado.withColumn("nivel_alfabetizacao", F.col("nivel_alfabetizacao").cast("string"))
    return resultado


def transformar_meta_brasil(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["ano", "rede"])


def transformar_meta_uf(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .dropDuplicates(["sigla_uf", "ano", "rede"])
    )


def transformar_alunos(df: DataFrame) -> DataFrame:
    derivado = F.when(F.col("proficiencia") >= PONTO_CORTE_ALFABETIZACAO, F.lit("1")).otherwise(F.lit("0"))
    return (
        df.withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("alfabetizado", F.coalesce(F.col("alfabetizado"), derivado))
        .dropDuplicates(["id_aluno", "ano"])
    )


TRANSFORMACOES = {
    "uf": transformar_uf,
    "municipio": transformar_municipio,
    "meta_brasil": transformar_meta_brasil,
    "meta_uf": transformar_meta_uf,
    "meta_municipio": transformar_meta_municipio,
    "alunos": transformar_alunos,
}
