"""Integração Silver: metas municipais + território + UF + streaming."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.silver.io_pandas import (
    adicionar_silver_timestamp,
    ler_parquet_s3,
    salvar_parquet_particionado,
)
from src.silver.transformacoes_pandas import transformar_indicador_streaming

logger = logging.getLogger(__name__)

ENTIDADE_INTEGRADA = "municipio_indicador_completo"


def _ler_silver_ou_bronze(
    s3_client,
    bucket_silver: str,
    bucket_bronze: str,
    entidade: str,
) -> pd.DataFrame:
    try:
        return ler_parquet_s3(s3_client, bucket_silver, f"silver/{entidade}/")
    except FileNotFoundError:
        from src.silver.processar_silver import _limpar_bronze
        from src.silver.transformacoes_pandas import TRANSFORMACOES_PANDAS

        bronze = _limpar_bronze(
            ler_parquet_s3(s3_client, bucket_bronze, f"bronze/batch/{entidade}/")
        )
        return TRANSFORMACOES_PANDAS[entidade](bronze)


def construir_integrado(
    meta_municipio: pd.DataFrame,
    municipio: pd.DataFrame,
    meta_uf: pd.DataFrame,
) -> pd.DataFrame:
    mun = municipio[["id_municipio", "nome_municipio", "nome_uf", "sigla_uf"]].rename(
        columns={"sigla_uf": "sigla_uf_municipio"}
    )
    uf = meta_uf[["sigla_uf", "ano", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "indicador_uf"}
    )
    uf = uf.drop_duplicates(subset=["sigla_uf", "ano"])

    integrado = meta_municipio.merge(mun, on="id_municipio", how="left")
    sigla = integrado.get("sigla_uf")
    if sigla is None:
        sigla = integrado["sigla_uf_municipio"]
    integrado["sigla_uf"] = sigla
    integrado = integrado.merge(uf, on=["sigla_uf", "ano"], how="left")
    integrado["_source_type"] = "batch"
    return integrado


def validar_referencial(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    quarentena_mask = pd.Series(False, index=df.index)
    motivos = pd.Series("", index=df.index, dtype=str)

    sem_municipio = df["nome_municipio"].isna()
    quarentena_mask |= sem_municipio
    motivos.loc[sem_municipio] = "municipio_inexistente"

    inconsistente = (
        df["nome_municipio"].notna()
        & df["sigla_uf_municipio"].notna()
        & (df["sigla_uf"].astype(str) != df["sigla_uf_municipio"].astype(str))
    )
    quarentena_mask |= inconsistente
    motivos.loc[inconsistente] = "uf_inconsistente"

    quarentena = df.loc[quarentena_mask].copy()
    validos = df.loc[~quarentena_mask].copy()
    if not quarentena.empty:
        quarentena["_motivo_quarentena"] = motivos.loc[quarentena.index]
    logger.info(
        "Integração referencial: %d órfãos, %d válidos",
        len(quarentena),
        len(validos),
    )
    return validos, quarentena


def merge_streaming(
    integrado_batch: pd.DataFrame,
    eventos_streaming: pd.DataFrame,
    municipio: pd.DataFrame,
    meta_uf: pd.DataFrame,
) -> pd.DataFrame:
    if eventos_streaming.empty:
        return integrado_batch

    cols_tecnicas = {
        "_ingestion_timestamp",
        "_ingestion_date",
        "_source_entity",
        "_job_name",
        "_record_hash",
        "_source_type",
    }
    eventos = eventos_streaming.drop(
        columns=[c for c in cols_tecnicas if c in eventos_streaming.columns]
    )
    eventos = transformar_indicador_streaming(eventos)
    eventos["rede"] = "total"
    eventos["_source_type"] = "streaming"

    mun = municipio[["id_municipio", "nome_municipio", "nome_uf", "sigla_uf"]].rename(
        columns={"sigla_uf": "sigla_uf_municipio"}
    )
    uf = meta_uf[["sigla_uf", "ano", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "indicador_uf"}
    )
    eventos = eventos.merge(mun, on="id_municipio", how="left")
    if "sigla_uf" not in eventos.columns or eventos["sigla_uf"].isna().all():
        eventos["sigla_uf"] = eventos.get("sigla_uf_municipio")
    eventos = eventos.merge(uf, on=["sigla_uf", "ano"], how="left")

    combinado = pd.concat([integrado_batch, eventos], ignore_index=True, sort=False)
    combinado = combinado.sort_values(
        by=["id_municipio", "ano", "rede", "_source_type"],
        ascending=[True, True, True, False],
    )
    return combinado.drop_duplicates(subset=["id_municipio", "ano", "rede"], keep="first")


def processar_integracao_silver(
    bucket_bronze: str,
    bucket_silver: str,
    s3_client,
) -> dict[str, Any]:
    meta_municipio = _ler_silver_ou_bronze(s3_client, bucket_silver, bucket_bronze, "meta_municipio")
    municipio = _ler_silver_ou_bronze(s3_client, bucket_silver, bucket_bronze, "municipio")
    meta_uf = _ler_silver_ou_bronze(s3_client, bucket_silver, bucket_bronze, "meta_uf")

    integrado = construir_integrado(meta_municipio, municipio, meta_uf)
    validos, quarentena_ref = validar_referencial(integrado)

    try:
        streaming = ler_parquet_s3(
            s3_client,
            bucket_bronze,
            "bronze/streaming/indicador_alfabetizacao/",
        )
    except FileNotFoundError:
        streaming = pd.DataFrame()
        logger.warning("Sem eventos streaming em bronze; integração apenas batch")

    final = merge_streaming(validos, streaming, municipio, meta_uf)
    final = adicionar_silver_timestamp(final)

    destino_silver = salvar_parquet_particionado(
        final,
        bucket_silver,
        f"silver/{ENTIDADE_INTEGRADA}/",
        s3_client,
        entidade=ENTIDADE_INTEGRADA,
    )
    destino_quarentena = None
    if not quarentena_ref.empty:
        destino_quarentena = salvar_parquet_particionado(
            quarentena_ref,
            bucket_silver,
            f"quarentena/{ENTIDADE_INTEGRADA}/",
            s3_client,
            entidade=ENTIDADE_INTEGRADA,
        )

    return {
        "entidade": ENTIDADE_INTEGRADA,
        "silver_linhas": len(final),
        "quarentena_linhas": len(quarentena_ref),
        "streaming_eventos": len(streaming),
        "destino_silver": destino_silver,
        "destino_quarentena": destino_quarentena,
    }
