"""Testes de verificacao da Fase 04 — ingestao batch."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.batch.extract_basedosdados import QUERIES_DIR, STAGING_DIR, extrair  # noqa: E402
from src.bronze.load_batch_pandas import construir_bronze_pandas  # noqa: E402
from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402
from src.dq.checks_pandas import checar_entidade_pandas  # noqa: E402

VOLUMES_ESPERADOS = {
    "uf": (27, 27),
    "municipio": (5000, 6000),
    "meta_brasil": (1, 5),
    "meta_uf": (50, 100),
    "meta_municipio": (10000, 11000),
    "alunos": (3_800_000, 3_900_000),
}

COLUNAS_OBRIGATORIAS = {
    "uf": {"sigla", "nome", "id_uf"},
    "municipio": {"id_municipio", "sigla_uf", "nome"},
    "meta_brasil": {"ano", "taxa_alfabetizacao"},
    "meta_uf": {"ano", "sigla_uf", "taxa_alfabetizacao"},
    "meta_municipio": {"ano", "id_municipio", "taxa_alfabetizacao"},
    "alunos": {"id_aluno", "id_municipio", "proficiencia", "alfabetizado"},
}

METADADOS_BRONZE = {
    "_ingestion_timestamp",
    "_ingestion_date",
    "_source_entity",
    "_job_name",
    "_record_hash",
}


def test_queries_existem() -> None:
    for entidade in ENTIDADES_BATCH:
        path = QUERIES_DIR / f"{entidade}.sql"
        assert path.exists(), f"Query ausente: {path}"
        conteudo = path.read_text(encoding="utf-8")
        assert "SELECT" in conteudo.upper()
        assert "SELECT *" not in conteudo.upper(), f"SELECT * em {entidade}"


def test_staging_parquet() -> None:
    for entidade in ENTIDADES_BATCH:
        path = STAGING_DIR / f"{entidade}.parquet"
        assert path.exists(), f"Staging ausente: {path}"
        df = pd.read_parquet(path)
        minimo, maximo = VOLUMES_ESPERADOS[entidade]
        assert minimo <= len(df) <= maximo, f"Volume {entidade}: {len(df)}"
        assert COLUNAS_OBRIGATORIAS[entidade].issubset(df.columns)
        if "id_municipio" in df.columns:
            amostra = df["id_municipio"].dropna().astype(str).head(5)
            assert all(len(v) == 7 and v.isdigit() for v in amostra)


def test_dq_bronze_local() -> None:
    for entidade in ENTIDADES_BATCH:
        df = pd.read_parquet(STAGING_DIR / f"{entidade}.parquet")
        df_bronze = construir_bronze_pandas(df, entidade)
        assert METADADOS_BRONZE.issubset(df_bronze.columns)
        checar_entidade_pandas(df_bronze, entidade, "bronze")


def _s3_client():
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    return boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    ), settings.bucket_bronze


def _glue_client():
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    return boto3.client("glue", region_name=settings.aws_default_region)


def _contar_linhas_s3(s3, bucket: str, prefix: str) -> int:
    total = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                path = tmp.name
            s3.download_file(bucket, obj["Key"], path)
            total += len(pd.read_parquet(path))
            os.remove(path)
    return total


def test_s3_bronze() -> None:
    s3, bucket = _s3_client()

    for entidade in ENTIDADES_BATCH:
        prefix = f"bronze/batch/{entidade}/"
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        parquet_keys = [k for k in keys if k.endswith(".parquet")]
        assert parquet_keys, f"Sem parquet bronze para {entidade}"
        assert any("/ano=" in k and "/mes=" in k and "/dia=" in k for k in parquet_keys)

        staging_n = len(pd.read_parquet(STAGING_DIR / f"{entidade}.parquet"))
        s3_n = _contar_linhas_s3(s3, bucket, prefix)
        assert s3_n >= staging_n, f"{entidade}: staging={staging_n} s3={s3_n}"
        assert s3_n % staging_n == 0, f"{entidade}: s3={s3_n} não é múltiplo de staging={staging_n}"


def test_idempotencia_extracao() -> None:
    """Re-extrai UF e compara contagem (idempotencia)."""
    path = STAGING_DIR / "uf_test_idempotencia.parquet"
    df1 = extrair("uf", destino=path)
    df2 = extrair("uf", destino=path)
    assert len(df1) == len(df2) == 27
    path.unlink(missing_ok=True)


def test_glue_catalog_por_entidade() -> None:
    glue = _glue_client()
    tables = {t["Name"] for t in glue.get_tables(DatabaseName="datalake_alfabetizacao")["TableList"]}
    bronze_esperadas = set(ENTIDADES_BATCH) | {"indicador_alfabetizacao"}
    silver_esperadas = {f"silver_{e}" for e in ENTIDADES_BATCH} | {"silver_municipio_indicador_completo"}
    assert bronze_esperadas.issubset(tables), f"Bronze ausente: {bronze_esperadas - tables}"
    assert silver_esperadas.issubset(tables), f"Silver ausente: {silver_esperadas - tables}"
    assert "batch" not in tables

    crawlers: list[str] = []
    token = None
    while True:
        kwargs = {"NextToken": token} if token else {}
        resp = glue.get_crawlers(**kwargs)
        crawlers.extend(c["Name"] for c in resp["Crawlers"])
        token = resp.get("NextToken")
        if not token:
            break

    assert "crawler-bronze-batch" not in crawlers
    for entidade in ENTIDADES_BATCH:
        nome = f"crawler-bronze-{entidade}"
        assert nome in crawlers, f"Crawler ausente: {nome}"

    parts_uf = glue.get_partitions(DatabaseName="datalake_alfabetizacao", TableName="uf")["Partitions"]
    assert len(parts_uf) >= 1
    parts_alunos = glue.get_partitions(DatabaseName="datalake_alfabetizacao", TableName="alunos")["Partitions"]
    assert len(parts_alunos) == 2


def test_metadados_s3_municipio() -> None:
    s3, bucket = _s3_client()
    key = "bronze/batch/municipio/ano=2026/mes=07/dia=12/part-00000.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        path = tmp.name
    s3.download_file(bucket, key, path)
    df = pd.read_parquet(path)
    os.remove(path)
    assert METADADOS_BRONZE.issubset(df.columns)
    assert df["_source_entity"].iloc[0] == "municipio"
    assert df["_job_name"].iloc[0] == "etl-bronze-batch"


def main() -> None:
    print("==> Teste 1: queries SQL")
    test_queries_existem()
    print("OK")

    print("==> Teste 2: staging parquet")
    test_staging_parquet()
    print("OK")

    print("==> Teste 3: DQ bronze local")
    test_dq_bronze_local()
    print("OK")

    print("==> Teste 4: S3 bronze (volumes)")
    test_s3_bronze()
    print("OK")

    print("==> Teste 5: idempotencia extracao UF")
    test_idempotencia_extracao()
    print("OK")

    print("==> Teste 6: Glue catalog por entidade")
    test_glue_catalog_por_entidade()
    print("OK")

    print("==> Teste 7: metadados S3 municipio")
    test_metadados_s3_municipio()
    print("OK")

    print("\nTODOS OS TESTES PASSARAM")


if __name__ == "__main__":
    main()
