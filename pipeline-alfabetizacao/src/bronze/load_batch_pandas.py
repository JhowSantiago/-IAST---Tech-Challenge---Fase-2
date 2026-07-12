"""Carga batch na camada Bronze sem dependência de Spark local."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd

ENTIDADES_TERRITORIAIS = {"uf", "municipio"}
JOB_NAME = "etl-bronze-batch"


def construir_bronze_pandas(df: pd.DataFrame, entidade: str) -> pd.DataFrame:
    agora = datetime.now(timezone.utc)
    resultado = df.copy()
    resultado["_ingestion_timestamp"] = agora
    resultado["_ingestion_date"] = agora.strftime("%Y-%m-%d")
    resultado["_source_entity"] = entidade
    resultado["_job_name"] = JOB_NAME

    colunas_hash = [c for c in resultado.columns if not c.startswith("_")]
    resultado["_record_hash"] = resultado[colunas_hash].astype(str).agg("|".join, axis=1).map(
        lambda valor: hashlib.md5(valor.encode("utf-8")).hexdigest()
    )

    if entidade in ENTIDADES_TERRITORIAIS:
        resultado["ano"] = str(agora.year)
        resultado["mes"] = f"{agora.month:02d}"
        resultado["dia"] = f"{agora.day:02d}"
    else:
        resultado["mes"] = f"{agora.month:02d}"
        resultado["dia"] = f"{agora.day:02d}"

    return resultado


def salvar_bronze_s3(
    df: pd.DataFrame,
    bucket_bronze: str,
    entidade: str,
    s3_client,
) -> str:
    destino = f"s3://{bucket_bronze}/bronze/batch/{entidade}/"
    for (ano, mes, dia), grupo in df.groupby(["ano", "mes", "dia"], sort=True):
        prefixo = f"bronze/batch/{entidade}/ano={ano}/mes={mes}/dia={dia}/part-00000.parquet"
        buffer = BytesIO()
        grupo.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket_bronze, prefixo)
    return destino
