"""Testes de verificação da Fase 06 — camada Silver."""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.batch.extract_basedosdados import STAGING_DIR  # noqa: E402
from src.common.config import get_settings  # noqa: E402
from src.dq.checks_pandas import checar_entidade_pandas  # noqa: E402
from src.silver.integracao_pandas import ENTIDADE_INTEGRADA, construir_integrado  # noqa: E402
from src.silver.transformacoes_pandas import (  # noqa: E402
    ENTIDADES_SILVER_BATCH,
    TRANSFORMACOES_PANDAS,
    transformar_meta_municipio,
)

VOLUMES_SILVER = {
    "uf": (27, 27),
    "municipio": (5000, 6000),
    "meta_brasil": (1, 5),
    "meta_uf": (50, 100),
    "meta_municipio": (10000, 11000),
    "alunos": (2_300_000, 2_400_000),
}

COLUNAS_SILVER = {
    "uf": {"sigla_uf", "nome_uf", "_silver_processed_at"},
    "municipio": {"id_municipio", "nome_municipio", "sigla_uf", "_silver_processed_at"},
    "meta_brasil": {"ano", "taxa_alfabetizacao", "_silver_processed_at"},
    "meta_uf": {"sigla_uf", "ano", "taxa_alfabetizacao", "_silver_processed_at"},
    "meta_municipio": {"id_municipio", "ano", "gap_meta", "atingiu_meta", "_silver_processed_at"},
    "alunos": {"id_aluno", "id_municipio", "proficiencia", "alfabetizado", "_silver_processed_at"},
}


def test_modulos_silver_existem() -> None:
    base = ROOT / "src" / "silver"
    for nome in (
        "transformacoes_pandas.py",
        "transformacoes.py",
        "processar_silver.py",
        "integracao_pandas.py",
        "etl-silver.py",
        "etl-silver-integracao.py",
        "io_pandas.py",
    ):
        assert (base / nome).exists(), f"Arquivo ausente: {nome}"


def test_transformacoes_locais() -> None:
    uf = TRANSFORMACOES_PANDAS["uf"](pd.read_parquet(STAGING_DIR / "uf.parquet"))
    assert set(uf["sigla_uf"]) == set(pd.read_parquet(STAGING_DIR / "uf.parquet")["sigla"].str.upper())
    assert len(uf) == 27

    municipio = TRANSFORMACOES_PANDAS["municipio"](pd.read_parquet(STAGING_DIR / "municipio.parquet"))
    assert municipio["id_municipio"].str.len().eq(7).all()

    meta_mun = transformar_meta_municipio(
        pd.read_parquet(STAGING_DIR / "meta_municipio.parquet").head(200),
        municipio,
    )
    assert "gap_meta" in meta_mun.columns
    assert "atingiu_meta" in meta_mun.columns

    alunos = TRANSFORMACOES_PANDAS["alunos"](pd.read_parquet(STAGING_DIR / "alunos.parquet").head(1000))
    checar_entidade_pandas(alunos.assign(_silver_processed_at=pd.Timestamp.utcnow()), "alunos", "silver")


def test_integracao_local() -> None:
    meta_municipio = transformar_meta_municipio(
        pd.read_parquet(STAGING_DIR / "meta_municipio.parquet").head(500),
        TRANSFORMACOES_PANDAS["municipio"](pd.read_parquet(STAGING_DIR / "municipio.parquet")),
    )
    municipio = TRANSFORMACOES_PANDAS["municipio"](pd.read_parquet(STAGING_DIR / "municipio.parquet"))
    meta_uf = TRANSFORMACOES_PANDAS["meta_uf"](pd.read_parquet(STAGING_DIR / "meta_uf.parquet"))
    integrado = construir_integrado(meta_municipio, municipio, meta_uf)
    assert "nome_municipio" in integrado.columns
    assert "indicador_uf" in integrado.columns
    assert integrado["nome_municipio"].notna().mean() > 0.9


def _s3():
    load_dotenv(ROOT / ".env")
    s = get_settings()
    return (
        boto3.client(
            "s3",
            region_name=s.aws_default_region,
            aws_access_key_id=s.aws_access_key_id,
            aws_secret_access_key=s.aws_secret_access_key,
        ),
        s,
    )


def test_silver_s3_prefixos() -> None:
    s3, settings = _s3()
    for entidade in ENTIDADES_SILVER_BATCH:
        resp = s3.list_objects_v2(
            Bucket=settings.bucket_silver,
            Prefix=f"silver/{entidade}/",
            MaxKeys=5,
        )
        assert resp.get("KeyCount", 0) > 0, f"Silver vazia: {entidade}"


def test_silver_volumes_s3() -> None:
    from src.silver.io_pandas import ler_parquet_s3

    s3, settings = _s3()
    for entidade in ("uf", "municipio", "meta_brasil", "meta_uf", "meta_municipio"):
        df = ler_parquet_s3(s3, settings.bucket_silver, f"silver/{entidade}/")
        minimo, maximo = VOLUMES_SILVER[entidade]
        assert minimo <= len(df) <= maximo, f"{entidade}: {len(df)}"
        assert COLUNAS_SILVER[entidade].issubset(df.columns)


def test_silver_integrada_s3() -> None:
    from src.silver.io_pandas import ler_parquet_s3

    s3, settings = _s3()
    df = ler_parquet_s3(s3, settings.bucket_silver, f"silver/{ENTIDADE_INTEGRADA}/")
    assert len(df) > 1000
    assert {"nome_municipio", "indicador_uf", "_source_type"}.issubset(df.columns)


def test_athena_silver_tabelas() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    glue = boto3.client(
        "glue",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    tabelas = {t["Name"] for t in glue.get_tables(DatabaseName="datalake_alfabetizacao")["TableList"]}
    esperadas = {f"silver_{e}" for e in ENTIDADES_SILVER_BATCH} | {f"silver_{ENTIDADE_INTEGRADA}"}
    faltando = esperadas - tabelas
    assert not faltando, f"Tabelas Athena ausentes: {faltando}"
