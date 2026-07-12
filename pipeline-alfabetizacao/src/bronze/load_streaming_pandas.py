"""Carga streaming na camada Bronze sem dependência de Spark local."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd

from src.streaming.eventos import ENTIDADE_STREAMING, JOB_NAME_STREAMING


def construir_bronze_streaming_pandas(df: pd.DataFrame) -> pd.DataFrame:
    agora = datetime.now(timezone.utc)
    resultado = df.copy()
    resultado["_ingestion_timestamp"] = agora
    resultado["_ingestion_date"] = agora.strftime("%Y-%m-%d")
    resultado["_source_entity"] = ENTIDADE_STREAMING
    resultado["_source_type"] = "streaming"
    resultado["_job_name"] = JOB_NAME_STREAMING

    colunas_hash = [c for c in resultado.columns if not c.startswith("_")]
    resultado["_record_hash"] = resultado[colunas_hash].astype(str).agg("|".join, axis=1).map(
        lambda valor: hashlib.md5(valor.encode("utf-8")).hexdigest()
    )
    resultado["mes"] = f"{agora.month:02d}"
    resultado["dia"] = f"{agora.day:02d}"
    return resultado


def salvar_bronze_streaming_s3(
    df: pd.DataFrame,
    bucket_bronze: str,
    s3_client,
) -> str:
    destino = f"s3://{bucket_bronze}/bronze/streaming/{ENTIDADE_STREAMING}/"
    for (ano, mes, dia), grupo in df.groupby(["ano", "mes", "dia"], sort=True):
        prefixo = (
            f"bronze/streaming/{ENTIDADE_STREAMING}/ano={ano}/mes={mes}/dia={dia}/"
            f"part-{hash((ano, mes, dia)) % 100000:05d}.parquet"
        )
        buffer = BytesIO()
        grupo.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket_bronze, prefixo)
    return destino
