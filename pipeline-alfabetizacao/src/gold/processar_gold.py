"""Processamento Silver → Gold (pandas)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.gold.agregacoes_pandas import AGREGACOES_PANDAS, VISOES_GOLD
from src.silver.io_pandas import ler_parquet_s3, salvar_parquet_particionado

logger = logging.getLogger(__name__)

ENTIDADE_INTEGRADA = "municipio_indicador_completo"


def _adicionar_gold_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["_gold_processed_at"] = datetime.now(timezone.utc)
    return resultado


def _validar_gold(df: pd.DataFrame, visao: str) -> None:
    if len(df) < 1:
        raise ValueError(f"Gold {visao}: dataset vazio")
    if visao == "indicador_municipio":
        obrigatorias = {"id_municipio", "pct_alfabetizados", "meta_pct", "gap_meta"}
    elif visao == "meta_vs_resultado":
        obrigatorias = {"sigla_uf", "ano", "taxa_media", "gap_medio"}
    else:
        obrigatorias = {"id_municipio", "ano", "pct_alfabetizados", "delta_percentual"}
    faltando = obrigatorias - set(df.columns)
    if faltando:
        raise ValueError(f"Gold {visao}: colunas ausentes {faltando}")


def processar_visao_gold(
    visao: str,
    bucket_silver: str,
    bucket_gold: str,
    s3_client,
    *,
    integrado: pd.DataFrame | None = None,
    alunos: pd.DataFrame | None = None,
    uf: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if visao not in AGREGACOES_PANDAS:
        raise KeyError(f"Visão Gold desconhecida: {visao}")

    if integrado is None:
        integrado = ler_parquet_s3(
            s3_client,
            bucket_silver,
            f"silver/{ENTIDADE_INTEGRADA}/",
        )

    agregar = AGREGACOES_PANDAS[visao]
    if visao == "indicador_municipio":
        if alunos is None:
            try:
                alunos = ler_parquet_s3(s3_client, bucket_silver, "silver/alunos/")
            except FileNotFoundError:
                alunos = pd.DataFrame()
        gold_df = agregar(integrado, alunos)
    elif visao == "meta_vs_resultado":
        if uf is None:
            try:
                uf = ler_parquet_s3(s3_client, bucket_silver, "silver/uf/")
            except FileNotFoundError:
                uf = pd.DataFrame()
        gold_df = agregar(integrado, uf)
    else:
        gold_df = agregar(integrado)

    gold_df = _adicionar_gold_timestamp(gold_df)
    _validar_gold(gold_df, visao)

    destino = salvar_parquet_particionado(
        gold_df,
        bucket_gold,
        f"gold/{visao}/",
        s3_client,
        entidade=visao,
    )
    resumo = {
        "visao": visao,
        "linhas": len(gold_df),
        "destino": destino,
    }
    logger.info("Gold %s: %d registros em %s", visao, resumo["linhas"], destino)
    return resumo


def processar_gold_completo(
    bucket_silver: str,
    bucket_gold: str,
    s3_client,
    visoes: list[str] | None = None,
) -> list[dict[str, Any]]:
    integrado = ler_parquet_s3(s3_client, bucket_silver, f"silver/{ENTIDADE_INTEGRADA}/")
    try:
        alunos = ler_parquet_s3(s3_client, bucket_silver, "silver/alunos/")
    except FileNotFoundError:
        alunos = pd.DataFrame()
    try:
        uf = ler_parquet_s3(s3_client, bucket_silver, "silver/uf/")
    except FileNotFoundError:
        uf = pd.DataFrame()

    resultados = []
    for visao in visoes or VISOES_GOLD:
        resultados.append(
            processar_visao_gold(
                visao,
                bucket_silver,
                bucket_gold,
                s3_client,
                integrado=integrado,
                alunos=alunos,
                uf=uf,
            )
        )
    return resultados
