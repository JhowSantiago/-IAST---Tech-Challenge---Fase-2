"""Processamento Bronze → Silver (pandas)."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.dq.checks_pandas import checar_entidade_pandas
from src.silver.io_pandas import (
    adicionar_silver_timestamp,
    ler_parquet_s3,
    salvar_parquet_particionado,
    separar_quarentena,
)
from src.silver.transformacoes_pandas import (
    ENTIDADES_SILVER_BATCH,
    TRANSFORMACOES_PANDAS,
)

logger = logging.getLogger(__name__)

COLUNAS_BRONZE_TECNICAS = {
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_entity",
    "_job_name",
    "_record_hash",
}


def _limpar_bronze(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[c for c in COLUNAS_BRONZE_TECNICAS if c in df.columns])


def processar_entidade_silver(
    entidade: str,
    bucket_bronze: str,
    bucket_silver: str,
    s3_client,
    *,
    fonte: str = "batch",
    municipios_silver: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if entidade not in TRANSFORMACOES_PANDAS:
        raise KeyError(f"Entidade sem transformação Silver: {entidade}")

    prefixo_bronze = f"bronze/{fonte}/{entidade}/"
    bronze = _limpar_bronze(ler_parquet_s3(s3_client, bucket_bronze, prefixo_bronze))

    transformar = TRANSFORMACOES_PANDAS[entidade]
    if entidade == "meta_municipio":
        silver_df = transformar(bronze, municipios_silver)
    else:
        silver_df = transformar(bronze)

    silver_df = adicionar_silver_timestamp(silver_df)
    validos, quarentena = separar_quarentena(silver_df, entidade, "silver")

    if not validos.empty:
        checar_entidade_pandas(validos, entidade, "silver")

    destino_silver = None
    destino_quarentena = None
    if not validos.empty:
        destino_silver = salvar_parquet_particionado(
            validos,
            bucket_silver,
            f"silver/{entidade}/",
            s3_client,
            entidade=entidade,
        )
    if not quarentena.empty:
        destino_quarentena = salvar_parquet_particionado(
            quarentena,
            bucket_silver,
            f"quarentena/{entidade}/",
            s3_client,
            entidade=entidade,
        )

    resumo = {
        "entidade": entidade,
        "fonte": fonte,
        "bronze_linhas": len(bronze),
        "silver_linhas": len(validos),
        "quarentena_linhas": len(quarentena),
        "destino_silver": destino_silver,
        "destino_quarentena": destino_quarentena,
    }
    logger.info(
        "Silver %s: %d válidos, %d quarentena",
        entidade,
        resumo["silver_linhas"],
        resumo["quarentena_linhas"],
    )
    return resumo


def processar_silver_batch(
    bucket_bronze: str,
    bucket_silver: str,
    s3_client,
    entidades: list[str] | None = None,
) -> list[dict[str, Any]]:
    alvo = entidades or ENTIDADES_SILVER_BATCH
    resultados: list[dict[str, Any]] = []
    municipios_silver: pd.DataFrame | None = None

    for entidade in alvo:
        if entidade == "meta_municipio":
            if municipios_silver is None:
                prefix = "silver/municipio/"
                try:
                    municipios_silver = ler_parquet_s3(s3_client, bucket_silver, prefix)
                except FileNotFoundError:
                    municipios_bronze = _limpar_bronze(
                        ler_parquet_s3(s3_client, bucket_bronze, "bronze/batch/municipio/")
                    )
                    municipios_silver = TRANSFORMACOES_PANDAS["municipio"](municipios_bronze)
        resultados.append(
            processar_entidade_silver(
                entidade,
                bucket_bronze,
                bucket_silver,
                s3_client,
                municipios_silver=municipios_silver,
            )
        )
    return resultados
