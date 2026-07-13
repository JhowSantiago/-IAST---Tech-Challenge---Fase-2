"""Registra tabelas Bronze no Glue Catalog (corrige colunas duplicadas dos crawlers)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402

DATABASE = "datalake_alfabetizacao"

PARQUET_SERDE = {
    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    "Parameters": {"serialization.format": "1"},
}

INPUT_OUTPUT = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
}

COLS_TECNICAS = [
    ("_ingestion_timestamp", "timestamp"),
    ("_ingestion_date", "string"),
    ("_source_entity", "string"),
    ("_job_name", "string"),
    ("_record_hash", "string"),
]

COLS_METAS = [
    ("meta_alfabetizacao_2024", "double"),
    ("meta_alfabetizacao_2025", "double"),
    ("meta_alfabetizacao_2026", "double"),
    ("meta_alfabetizacao_2027", "double"),
    ("meta_alfabetizacao_2028", "double"),
    ("meta_alfabetizacao_2029", "double"),
    ("meta_alfabetizacao_2030", "double"),
]

PARTITIONS_AVALIACAO = [("ano", "string"), ("mes", "string"), ("dia", "string")]

TABELAS = {
    "uf": {
        "location_suffix": "bronze/batch/uf/",
        "columns": [
            ("id_uf", "string"),
            ("sigla", "string"),
            ("nome", "string"),
            ("regiao", "string"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
    "municipio": {
        "location_suffix": "bronze/batch/municipio/",
        "columns": [
            ("id_municipio", "string"),
            ("sigla_uf", "string"),
            ("nome", "string"),
            ("nome_uf", "string"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
    "meta_brasil": {
        "location_suffix": "bronze/batch/meta_brasil/",
        "columns": [
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            *COLS_METAS,
            ("percentual_participacao", "double"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
    "meta_uf": {
        "location_suffix": "bronze/batch/meta_uf/",
        "columns": [
            ("sigla_uf", "string"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            *COLS_METAS,
            ("percentual_participacao", "double"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
    "meta_municipio": {
        "location_suffix": "bronze/batch/meta_municipio/",
        "columns": [
            ("id_municipio", "string"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            *COLS_METAS,
            ("nivel_alfabetizacao", "string"),
            ("percentual_participacao", "double"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
    "alunos": {
        "location_suffix": "bronze/batch/alunos/",
        "columns": [
            ("id_municipio", "string"),
            ("id_escola", "string"),
            ("id_aluno", "string"),
            ("caderno", "string"),
            ("serie", "string"),
            ("rede", "string"),
            ("presenca", "string"),
            ("preenchimento_caderno", "string"),
            ("alfabetizado", "string"),
            ("proficiencia", "double"),
            ("peso_aluno", "double"),
            *COLS_TECNICAS,
        ],
        "partitions": PARTITIONS_AVALIACAO,
    },
}


def _cols(definicao: list[tuple[str, str]]) -> list[dict]:
    return [{"Name": nome, "Type": tipo} for nome, tipo in definicao]


def _criar_ou_atualizar(glue, nome: str, bucket: str, spec: dict) -> None:
    location = f"s3://{bucket}/{spec['location_suffix']}"
    table_input = {
        "Name": nome,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "parquet.compression": "SNAPPY",
        },
        "StorageDescriptor": {
            "Columns": _cols(spec["columns"]),
            "Location": location,
            **INPUT_OUTPUT,
            "SerdeInfo": PARQUET_SERDE,
        },
        "PartitionKeys": _cols(spec["partitions"]),
    }
    try:
        glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"CREATE: {nome}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"UPDATE: {nome}")


def _aguardar_athena(athena, query_id: str) -> str:
    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_id)
        estado = resp["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if estado["State"] != "SUCCEEDED":
                raise RuntimeError(estado.get("StateChangeReason", estado["State"]))
            return resp["QueryExecution"]["Statistics"].get("DataScannedInBytes", 0)
        time.sleep(1)


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    if not settings.bucket_bronze:
        raise ValueError("BUCKET_BRONZE não configurado")

    session = boto3.Session(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )
    glue = session.client("glue")
    athena = session.client("athena")
    output = f"s3://{settings.bucket_bronze}/athena-results/"

    for nome, spec in TABELAS.items():
        _criar_ou_atualizar(glue, nome, settings.bucket_bronze, spec)

    for nome in TABELAS:
        sql = f"MSCK REPAIR TABLE {DATABASE}.{nome}"
        resp = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": output},
        )
        _aguardar_athena(athena, resp["QueryExecutionId"])
        print(f"REPAIR: {nome}")

    for nome in ("meta_brasil", "meta_municipio", "meta_uf"):
        sql = f'SELECT * FROM {DATABASE}.{nome} LIMIT 1'
        resp = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": output},
        )
        bytes_lidos = _aguardar_athena(athena, resp["QueryExecutionId"])
        print(f"OK: {nome} ({bytes_lidos} bytes scanned)")


if __name__ == "__main__":
    main()
