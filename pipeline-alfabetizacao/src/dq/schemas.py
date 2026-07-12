"""Schemas PySpark explícitos por entidade e camada."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

META_COLUNAS = [
    StructField("meta_alfabetizacao_2024", DoubleType(), True),
    StructField("meta_alfabetizacao_2025", DoubleType(), True),
    StructField("meta_alfabetizacao_2026", DoubleType(), True),
    StructField("meta_alfabetizacao_2027", DoubleType(), True),
    StructField("meta_alfabetizacao_2028", DoubleType(), True),
    StructField("meta_alfabetizacao_2029", DoubleType(), True),
    StructField("meta_alfabetizacao_2030", DoubleType(), True),
]

BRONZE_TECNICOS = [
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_ingestion_date", StringType(), False),
    StructField("_source_entity", StringType(), False),
    StructField("_job_name", StringType(), False),
    StructField("_record_hash", StringType(), True),
]

SILVER_TECNICOS = [
    StructField("_silver_processed_at", TimestampType(), False),
]

GOLD_TECNICOS = [
    StructField("_gold_processed_at", TimestampType(), False),
]

SCHEMAS: dict[str, dict[str, StructType]] = {
    "uf": {
        "bronze": StructType(
            [
                StructField("id_uf", StringType(), True),
                StructField("sigla", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("regiao", StringType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("id_uf", StringType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("nome_uf", StringType(), False),
                StructField("regiao", StringType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("sigla_uf", StringType(), False),
                StructField("nome_uf", StringType(), False),
                StructField("regiao", StringType(), True),
                *GOLD_TECNICOS,
            ]
        ),
    },
    "municipio": {
        "bronze": StructType(
            [
                StructField("id_municipio", StringType(), True),
                StructField("sigla_uf", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("nome_uf", StringType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("id_municipio", StringType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("nome_municipio", StringType(), False),
                StructField("nome_uf", StringType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("id_municipio", StringType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("nome_municipio", StringType(), False),
                *GOLD_TECNICOS,
            ]
        ),
    },
    "meta_brasil": {
        "bronze": StructType(
            [
                StructField("ano", IntegerType(), True),
                StructField("rede", StringType(), True),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("percentual_participacao", DoubleType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("rede", StringType(), False),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("percentual_participacao", DoubleType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("rede", StringType(), False),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                StructField("meta_vigente", DoubleType(), True),
                StructField("gap_meta", DoubleType(), True),
                StructField("atingiu_meta", BooleanType(), True),
                *GOLD_TECNICOS,
            ]
        ),
    },
    "meta_uf": {
        "bronze": StructType(
            [
                StructField("ano", IntegerType(), True),
                StructField("sigla_uf", StringType(), True),
                StructField("rede", StringType(), True),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("percentual_participacao", DoubleType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("rede", StringType(), False),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("percentual_participacao", DoubleType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                StructField("meta_vigente", DoubleType(), True),
                StructField("gap_meta", DoubleType(), True),
                StructField("atingiu_meta", BooleanType(), True),
                *GOLD_TECNICOS,
            ]
        ),
    },
    "meta_municipio": {
        "bronze": StructType(
            [
                StructField("ano", IntegerType(), True),
                StructField("id_municipio", StringType(), True),
                StructField("rede", StringType(), True),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("nivel_alfabetizacao", StringType(), True),
                StructField("percentual_participacao", DoubleType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("id_municipio", StringType(), False),
                StructField("sigla_uf", StringType(), True),
                StructField("rede", StringType(), False),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                *META_COLUNAS,
                StructField("nivel_alfabetizacao", StringType(), True),
                StructField("percentual_participacao", DoubleType(), True),
                StructField("gap_meta", DoubleType(), True),
                StructField("atingiu_meta", BooleanType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("id_municipio", StringType(), False),
                StructField("sigla_uf", StringType(), False),
                StructField("nome_municipio", StringType(), True),
                StructField("taxa_alfabetizacao", DoubleType(), True),
                StructField("meta_vigente", DoubleType(), True),
                StructField("gap_meta", DoubleType(), True),
                StructField("atingiu_meta", BooleanType(), True),
                StructField("delta_anual", DoubleType(), True),
                *GOLD_TECNICOS,
            ]
        ),
    },
    "alunos": {
        "bronze": StructType(
            [
                StructField("ano", IntegerType(), True),
                StructField("id_municipio", StringType(), True),
                StructField("id_escola", StringType(), True),
                StructField("id_aluno", StringType(), True),
                StructField("caderno", StringType(), True),
                StructField("serie", StringType(), True),
                StructField("rede", StringType(), True),
                StructField("presenca", StringType(), True),
                StructField("preenchimento_caderno", StringType(), True),
                StructField("alfabetizado", StringType(), True),
                StructField("proficiencia", DoubleType(), True),
                StructField("peso_aluno", DoubleType(), True),
                *BRONZE_TECNICOS,
            ]
        ),
        "silver": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("id_municipio", StringType(), False),
                StructField("id_escola", StringType(), True),
                StructField("id_aluno", StringType(), False),
                StructField("serie", StringType(), True),
                StructField("rede", StringType(), True),
                StructField("alfabetizado", StringType(), True),
                StructField("proficiencia", DoubleType(), True),
                StructField("peso_aluno", DoubleType(), True),
                *SILVER_TECNICOS,
            ]
        ),
        "gold": StructType(
            [
                StructField("ano", IntegerType(), False),
                StructField("id_municipio", StringType(), False),
                StructField("total_alunos", IntegerType(), False),
                StructField("total_alfabetizados", IntegerType(), False),
                StructField("taxa_alfabetizacao_calc", DoubleType(), True),
                *GOLD_TECNICOS,
            ]
        ),
    },
}


def get_schema(entidade: str, camada: str) -> StructType:
    """Retorna o StructType da entidade na camada solicitada."""
    camada = camada.lower()
    if entidade not in SCHEMAS:
        raise KeyError(f"Entidade desconhecida: {entidade}")
    if camada not in SCHEMAS[entidade]:
        raise KeyError(f"Camada '{camada}' não definida para entidade '{entidade}'")
    return SCHEMAS[entidade][camada]
