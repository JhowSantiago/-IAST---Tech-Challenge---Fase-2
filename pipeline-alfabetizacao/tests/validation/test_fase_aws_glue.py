"""Testes de deploy AWS Glue."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

JOBS_ESPERADOS = [
    "etl-bronze-batch-uf",
    "etl-bronze-batch-alunos",
    "etl-silver-integracao",
    "etl-gold-indicador_municipio",
    "etl-gold-meta_vs_resultado",
    "etl-gold-evolucao_temporal",
]


def _glue():
    load_dotenv(ROOT / ".env")
    return boto3.client(
        "glue",
        region_name=os.getenv("AWS_DEFAULT_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def test_scripts_deploy_existem() -> None:
    for nome in (
        "publicar_glue_aws.py",
        "provisionar_jobs_glue.py",
        "executar_pipeline_aws.py",
        "provisionar_usuario_viewer.py",
        "deploy_aws.py",
    ):
        assert (ROOT / "scripts" / nome).exists()


def test_glue_jobs_provisionados() -> None:
    glue = _glue()
    existentes = {j["Name"] for j in glue.get_jobs()["Jobs"]}
    for nome in JOBS_ESPERADOS:
        assert nome in existentes, f"Job ausente: {nome}"


def test_glue_assets_s3() -> None:
    load_dotenv(ROOT / ".env")
    s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket = os.environ["BUCKET_BRONZE"]
    for key in (
        "glue/libs/pipeline_src.zip",
        "glue/scripts/etl-gold.py",
    ):
        s3.head_object(Bucket=bucket, Key=key)


def test_gold_jobs_sucesso_recente() -> None:
    glue = _glue()
    for job in (
        "etl-gold-indicador_municipio",
        "etl-gold-meta_vs_resultado",
        "etl-gold-evolucao_temporal",
    ):
        run = glue.get_job_runs(JobName=job, MaxResults=1)["JobRuns"][0]
        assert run["JobRunState"] == "SUCCEEDED", f"{job}: {run['JobRunState']}"


def test_usuario_viewer_existe() -> None:
    load_dotenv(ROOT / ".env")
    iam = boto3.client(
        "iam",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    iam.get_user(UserName="alfabetizacao-viewer")
    policies = iam.list_user_policies(UserName="alfabetizacao-viewer")["PolicyNames"]
    assert "alfabetizacao-viewer-readonly" in policies
