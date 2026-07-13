"""Registra tabelas Silver no Glue Catalog e executa MSCK REPAIR no Athena."""

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

TABELAS = {
    "silver_uf": {
        "location_suffix": "silver/uf/",
        "columns": [
            ("id_uf", "string"),
            ("sigla_uf", "string"),
            ("nome_uf", "string"),
            ("regiao", "string"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("ano", "string"), ("mes", "string"), ("dia", "string")],
    },
    "silver_municipio": {
        "location_suffix": "silver/municipio/",
        "columns": [
            ("id_municipio", "string"),
            ("sigla_uf", "string"),
            ("nome_municipio", "string"),
            ("nome_uf", "string"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("ano", "string"), ("mes", "string"), ("dia", "string")],
    },
    "silver_meta_brasil": {
        "location_suffix": "silver/meta_brasil/",
        "columns": [
            ("ano", "int"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            ("meta_alfabetizacao_2024", "double"),
            ("meta_alfabetizacao_2025", "double"),
            ("meta_alfabetizacao_2026", "double"),
            ("meta_alfabetizacao_2027", "double"),
            ("meta_alfabetizacao_2028", "double"),
            ("meta_alfabetizacao_2029", "double"),
            ("meta_alfabetizacao_2030", "double"),
            ("percentual_participacao", "double"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "silver_meta_uf": {
        "location_suffix": "silver/meta_uf/",
        "columns": [
            ("ano", "int"),
            ("sigla_uf", "string"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            ("meta_alfabetizacao_2024", "double"),
            ("meta_alfabetizacao_2025", "double"),
            ("meta_alfabetizacao_2026", "double"),
            ("meta_alfabetizacao_2027", "double"),
            ("meta_alfabetizacao_2028", "double"),
            ("meta_alfabetizacao_2029", "double"),
            ("meta_alfabetizacao_2030", "double"),
            ("percentual_participacao", "double"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "silver_meta_municipio": {
        "location_suffix": "silver/meta_municipio/",
        "columns": [
            ("ano", "int"),
            ("id_municipio", "string"),
            ("sigla_uf", "string"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            ("meta_alfabetizacao_2024", "double"),
            ("meta_alfabetizacao_2025", "double"),
            ("meta_alfabetizacao_2026", "double"),
            ("meta_alfabetizacao_2027", "double"),
            ("meta_alfabetizacao_2028", "double"),
            ("meta_alfabetizacao_2029", "double"),
            ("meta_alfabetizacao_2030", "double"),
            ("nivel_alfabetizacao", "string"),
            ("percentual_participacao", "double"),
            ("gap_meta", "double"),
            ("atingiu_meta", "boolean"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "silver_alunos": {
        "location_suffix": "silver/alunos/",
        "columns": [
            ("ano", "int"),
            ("id_municipio", "string"),
            ("id_escola", "string"),
            ("id_aluno", "string"),
            ("serie", "string"),
            ("rede", "string"),
            ("alfabetizado", "string"),
            ("proficiencia", "double"),
            ("peso_aluno", "double"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "silver_municipio_indicador_completo": {
        "location_suffix": "silver/municipio_indicador_completo/",
        "columns": [
            ("ano", "int"),
            ("id_municipio", "string"),
            ("sigla_uf", "string"),
            ("rede", "string"),
            ("taxa_alfabetizacao", "double"),
            ("meta_alfabetizacao_2024", "double"),
            ("meta_alfabetizacao_2025", "double"),
            ("meta_alfabetizacao_2026", "double"),
            ("meta_alfabetizacao_2027", "double"),
            ("meta_alfabetizacao_2028", "double"),
            ("meta_alfabetizacao_2029", "double"),
            ("meta_alfabetizacao_2030", "double"),
            ("nivel_alfabetizacao", "string"),
            ("percentual_participacao", "double"),
            ("gap_meta", "double"),
            ("atingiu_meta", "boolean"),
            ("nome_municipio", "string"),
            ("nome_uf", "string"),
            ("indicador_uf", "double"),
            ("event_id", "string"),
            ("_source_type", "string"),
            ("_silver_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
}


def _cols(definicao: list[tuple[str, str]]) -> list[dict]:
    return [{"Name": nome, "Type": tipo} for nome, tipo in definicao]


def _part_keys(definicao: list[tuple[str, str]]) -> list[dict]:
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
        "PartitionKeys": _part_keys(spec["partitions"]),
    }
    try:
        glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"CREATE: {nome}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"UPDATE: {nome}")


def _aguardar_athena(athena, query_id: str) -> None:
    while True:
        estado = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if estado["State"] != "SUCCEEDED":
                raise RuntimeError(estado.get("StateChangeReason", estado["State"]))
            return
        time.sleep(1)


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    if not settings.bucket_silver:
        raise ValueError("BUCKET_SILVER não configurado")

    session = boto3.Session(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )
    glue = session.client("glue")
    athena = session.client("athena")
    output = f"s3://{settings.bucket_silver}/athena-results/"

    for nome, spec in TABELAS.items():
        _criar_ou_atualizar(glue, nome, settings.bucket_silver, spec)

    for nome in TABELAS:
        sql = f"MSCK REPAIR TABLE {DATABASE}.{nome}"
        resp = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": output},
        )
        _aguardar_athena(athena, resp["QueryExecutionId"])
        print(f"REPAIR: {nome}")


if __name__ == "__main__":
    main()
