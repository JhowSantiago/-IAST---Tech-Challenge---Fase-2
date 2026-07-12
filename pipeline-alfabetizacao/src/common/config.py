"""Configuração centralizada do pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()

ENTIDADES_BATCH: list[str] = [
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
    "alunos",
]

# Mapeamento entidade interna → dataset.tabela na Base dos Dados (BigQuery)
FONTES_BIGQUERY: dict[str, tuple[str, str]] = {
    "uf": ("br_bd_diretorios_brasil", "uf"),
    "municipio": ("br_bd_diretorios_brasil", "municipio"),
    "meta_brasil": ("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_brasil"),
    "meta_uf": ("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_uf"),
    "meta_municipio": ("br_inep_avaliacao_alfabetizacao", "meta_alfabetizacao_municipio"),
    "indicador_uf": ("br_inep_avaliacao_alfabetizacao", "uf"),
    "indicador_municipio": ("br_inep_avaliacao_alfabetizacao", "municipio"),
    "alunos": ("br_inep_avaliacao_alfabetizacao", "alunos"),
}


@dataclass(frozen=True)
class Settings:
    billing_project_id: str = field(
        default_factory=lambda: os.getenv("BILLING_PROJECT_ID", "")
    )
    aws_access_key_id: str = field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID", "")
    )
    aws_secret_access_key: str = field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY", "")
    )
    aws_default_region: str = field(
        default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )
    bucket_bronze: str = field(default_factory=lambda: os.getenv("BUCKET_BRONZE", ""))
    bucket_silver: str = field(default_factory=lambda: os.getenv("BUCKET_SILVER", ""))
    bucket_gold: str = field(default_factory=lambda: os.getenv("BUCKET_GOLD", ""))
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    entidades_batch: tuple[str, ...] = field(default_factory=lambda: tuple(ENTIDADES_BATCH))

    def bronze_batch_path(self, entidade: str, ano: str, mes: str, dia: str) -> str:
        return (
            f"s3://{self.bucket_bronze}/bronze/batch/{entidade}/"
            f"ano={ano}/mes={mes}/dia={dia}/"
        )

    def bronze_streaming_path(self, entidade: str, ano: str, mes: str, dia: str) -> str:
        return (
            f"s3://{self.bucket_bronze}/bronze/streaming/{entidade}/"
            f"ano={ano}/mes={mes}/dia={dia}/"
        )

    def silver_path(self, entidade: str) -> str:
        return f"s3://{self.bucket_silver}/silver/{entidade}/"

    def gold_path(self, visao: str) -> str:
        return f"s3://{self.bucket_gold}/gold/{visao}/"

    def quarentena_path(self, entidade: str) -> str:
        return f"s3://{self.bucket_silver}/quarentena/{entidade}/"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
