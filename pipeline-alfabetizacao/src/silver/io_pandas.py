"""Utilitários de leitura/escrita e quarentena da camada Silver."""

from __future__ import annotations

import re
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from src.dq.checks import get_checks

PARTITION_COLS_TERRITORIAL = ("ano", "mes", "dia")
PARTITION_COLS_PADRAO = ("mes", "dia")


def particoes_entidade(entidade: str) -> tuple[str, ...]:
    if entidade in {"uf", "municipio"}:
        return PARTITION_COLS_TERRITORIAL
    return PARTITION_COLS_PADRAO


def _entidade_do_prefixo(prefix: str) -> str:
    partes = prefix.strip("/").split("/")
    return partes[-1] if partes else ""


def _extrair_particoes_da_chave(key: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in re.finditer(r"(ano|mes|dia)=([^/]+)", key)}


def ler_parquet_s3(s3_client, bucket: str, prefix: str) -> pd.DataFrame:
    entidade = _entidade_do_prefixo(prefix)
    part_cols = particoes_entidade(entidade)
    frames: list[pd.DataFrame] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                path = tmp.name
            s3_client.download_file(bucket, key, path)
            part = pd.read_parquet(path)
            Path(path).unlink(missing_ok=True)
            for col, valor in _extrair_particoes_da_chave(key).items():
                if col in part_cols and col not in part.columns:
                    part[col] = valor
            frames.append(part)
    if not frames:
        raise FileNotFoundError(f"Nenhum parquet em s3://{bucket}/{prefix}")
    return pd.concat(frames, ignore_index=True)


def adicionar_particoes_ingestao(df: pd.DataFrame, entidade: str) -> pd.DataFrame:
    agora = datetime.now(timezone.utc)
    resultado = df.copy()
    if entidade in {"uf", "municipio"} and "ano" not in resultado.columns:
        resultado["ano"] = str(agora.year)
    resultado["mes"] = f"{agora.month:02d}"
    resultado["dia"] = f"{agora.day:02d}"
    return resultado


def adicionar_silver_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    resultado = df.copy()
    resultado["_silver_processed_at"] = datetime.now(timezone.utc)
    return resultado


def _falha_linha(serie: pd.Series, check: dict[str, Any]) -> pd.Series:
    tipo = check["tipo"]
    coluna = check.get("coluna")
    if tipo == "not_null":
        return serie[coluna].isna()
    if tipo == "regex":
        padrao = check["valor"]
        vals = serie[coluna].astype(str)
        return serie[coluna].isna() | ~vals.map(lambda v: bool(re.fullmatch(padrao, v)))
    if tipo == "range":
        vals = pd.to_numeric(serie[coluna], errors="coerce")
        mask = pd.Series(False, index=serie.index)
        if check.get("minimo") is not None:
            mask |= vals < check["minimo"]
        if check.get("maximo") is not None:
            mask |= vals > check["maximo"]
        return mask & vals.notna()
    return pd.Series(False, index=serie.index)


def separar_quarentena(
    df: pd.DataFrame,
    entidade: str,
    camada: str = "silver",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    checks = [c for c in get_checks(entidade, camada) if c.get("critico", True)]
    invalid_mask = pd.Series(False, index=df.index)
    motivos = pd.Series("", index=df.index, dtype=str)

    for check in checks:
        if check["tipo"] == "unique":
            coluna = check["coluna"]
            falha = df.duplicated(subset=[coluna], keep="first")
        elif check["tipo"] == "min_count":
            continue
        else:
            falha = _falha_linha(df, check)
        nome = check.get("coluna", check["tipo"])
        motivos.loc[falha] = motivos.loc[falha] + f";{nome}"
        invalid_mask |= falha

    quarentena = df.loc[invalid_mask].copy()
    validos = df.loc[~invalid_mask].copy()
    if not quarentena.empty:
        quarentena["_motivo_quarentena"] = motivos.loc[quarentena.index].str.strip(";")
    return validos, quarentena


def salvar_parquet_particionado(
    df: pd.DataFrame,
    bucket: str,
    prefixo_base: str,
    s3_client,
    *,
    entidade: str | None = None,
) -> str:
    entidade = entidade or _entidade_do_prefixo(prefixo_base)
    part_cols = particoes_entidade(entidade)
    destino = f"s3://{bucket}/{prefixo_base}"
    df = adicionar_particoes_ingestao(df, entidade)
    for chaves, grupo in df.groupby(list(part_cols), sort=True):
        if not isinstance(chaves, tuple):
            chaves = (chaves,)
        partes = dict(zip(part_cols, chaves))
        part_path = "/".join(f"{k}={v}" for k, v in partes.items())
        key = f"{prefixo_base.rstrip('/')}/{part_path}/part-00000.parquet"
        dados = grupo.drop(columns=list(part_cols), errors="ignore")
        buffer = BytesIO()
        dados.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket, key)
    return destino
