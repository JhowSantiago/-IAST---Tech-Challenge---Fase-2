"""Testes de verificação da Fase 07 — camada Gold."""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.gold.agregacoes_pandas import (  # noqa: E402
    VISOES_GOLD,
    agregar_evolucao_temporal,
    agregar_indicador_municipio,
    agregar_meta_vs_resultado,
)
from src.gold.processar_gold import processar_gold_completo  # noqa: E402
from src.silver.integracao_pandas import construir_integrado  # noqa: E402
from src.silver.transformacoes_pandas import TRANSFORMACOES_PANDAS  # noqa: E402

STAGING = ROOT / "data" / "staging"


def _dados_locais() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    meta_municipio = TRANSFORMACOES_PANDAS["meta_municipio"](
        pd.read_parquet(STAGING / "meta_municipio.parquet").head(500),
        TRANSFORMACOES_PANDAS["municipio"](pd.read_parquet(STAGING / "municipio.parquet")),
    )
    municipio = TRANSFORMACOES_PANDAS["municipio"](pd.read_parquet(STAGING / "municipio.parquet"))
    meta_uf = TRANSFORMACOES_PANDAS["meta_uf"](pd.read_parquet(STAGING / "meta_uf.parquet"))
    integrado = construir_integrado(meta_municipio, municipio, meta_uf)
    uf = TRANSFORMACOES_PANDAS["uf"](pd.read_parquet(STAGING / "uf.parquet"))
    alunos = TRANSFORMACOES_PANDAS["alunos"](pd.read_parquet(STAGING / "alunos.parquet").head(5000))
    return integrado, uf, alunos


def test_modulos_gold_existem() -> None:
    base = ROOT / "src" / "gold"
    for nome in (
        "agregacoes_pandas.py",
        "agregacoes.py",
        "processar_gold.py",
        "etl-gold.py",
    ):
        assert (base / nome).exists(), f"Arquivo ausente: {nome}"
    assert (ROOT / "scripts" / "carregar_gold.py").exists()
    assert (ROOT / "sql" / "athena" / "gold_ddl.sql").exists()
    assert (ROOT / "sql" / "athena" / "analises.sql").exists()


def test_agregacoes_locais() -> None:
    integrado, uf, alunos = _dados_locais()
    ind = agregar_indicador_municipio(integrado, alunos)
    meta = agregar_meta_vs_resultado(integrado, uf)
    evo = agregar_evolucao_temporal(integrado)

    assert len(ind) > 0
    assert {"pct_alfabetizados", "meta_pct", "gap_meta", "atingiu_meta"}.issubset(ind.columns)
    assert len(meta) > 0
    assert {"taxa_media", "ranking_taxa", "municipios_acima_meta"}.issubset(meta.columns)
    assert len(evo) > 0
    assert "delta_percentual" in evo.columns
    assert len(ind) <= len(integrado)


def test_gold_s3_prefixos() -> None:
    load_dotenv(ROOT / ".env")
    from src.common.config import get_settings

    settings = get_settings()
    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    for visao in VISOES_GOLD:
        resp = s3.list_objects_v2(
            Bucket=settings.bucket_gold,
            Prefix=f"gold/{visao}/",
            MaxKeys=3,
        )
        assert resp.get("KeyCount", 0) > 0, f"Gold vazia: {visao}"


def test_gold_volumes_s3() -> None:
    from src.common.config import get_settings
    from src.silver.io_pandas import ler_parquet_s3

    load_dotenv(ROOT / ".env")
    settings = get_settings()
    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    ind = ler_parquet_s3(s3, settings.bucket_gold, "gold/indicador_municipio/")
    meta = ler_parquet_s3(s3, settings.bucket_gold, "gold/meta_vs_resultado/")
    evo = ler_parquet_s3(s3, settings.bucket_gold, "gold/evolucao_temporal/")

    assert 1000 < len(ind) < 11000
    assert 40 <= len(meta) <= 60
    assert 1000 < len(evo) < 11000
    assert len(ind) < 10754


def test_athena_gold_tabelas() -> None:
    load_dotenv(ROOT / ".env")
    from src.common.config import get_settings

    settings = get_settings()
    glue = boto3.client(
        "glue",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    tabelas = {t["Name"] for t in glue.get_tables(DatabaseName="datalake_alfabetizacao")["TableList"]}
    esperadas = {
        "gold_indicador_municipio",
        "gold_meta_vs_resultado",
        "gold_evolucao_temporal",
    }
    assert esperadas.issubset(tabelas), f"Gold ausente: {esperadas - tabelas}"
