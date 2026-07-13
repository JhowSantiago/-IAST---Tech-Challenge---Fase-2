"""Registra tabelas Gold no Glue Catalog e executa MSCK REPAIR."""

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
    "gold_indicador_municipio": {
        "location_suffix": "gold/indicador_municipio/",
        "columns": [
            ("id_municipio", "string"),
            ("nome", "string"),
            ("sigla_uf", "string"),
            ("nome_uf", "string"),
            ("ano", "int"),
            ("rede", "string"),
            ("pct_alfabetizados", "double"),
            ("meta_pct", "double"),
            ("gap_meta", "double"),
            ("atingiu_meta", "boolean"),
            ("indicador_uf", "double"),
            ("nivel_alfabetizacao", "string"),
            ("percentual_participacao", "double"),
            ("total_alunos_avaliados", "bigint"),
            ("_gold_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "gold_meta_vs_resultado": {
        "location_suffix": "gold/meta_vs_resultado/",
        "columns": [
            ("sigla_uf", "string"),
            ("ano", "int"),
            ("taxa_media", "double"),
            ("meta_media", "double"),
            ("gap_medio", "double"),
            ("municipios_total", "bigint"),
            ("municipios_acima_meta", "bigint"),
            ("municipios_abaixo_meta", "bigint"),
            ("ranking_taxa", "int"),
            ("taxa_anterior", "double"),
            ("delta_percentual", "double"),
            ("regiao", "string"),
            ("nome_uf", "string"),
            ("_gold_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
    "gold_evolucao_temporal": {
        "location_suffix": "gold/evolucao_temporal/",
        "columns": [
            ("id_municipio", "string"),
            ("nome", "string"),
            ("sigla_uf", "string"),
            ("nome_uf", "string"),
            ("rede", "string"),
            ("ano", "int"),
            ("pct_alfabetizados", "double"),
            ("pct_anterior", "double"),
            ("delta_percentual", "double"),
            ("delta_anual", "double"),
            ("meta_pct", "double"),
            ("gap_meta", "double"),
            ("atingiu_meta", "boolean"),
            ("_gold_processed_at", "timestamp"),
        ],
        "partitions": [("mes", "string"), ("dia", "string")],
    },
}


def _criar_ou_atualizar(glue, nome: str, bucket: str, spec: dict) -> None:
    location = f"s3://{bucket}/{spec['location_suffix']}"
    table_input = {
        "Name": nome,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "parquet.compression": "SNAPPY"},
        "StorageDescriptor": {
            "Columns": [{"Name": c, "Type": t} for c, t in spec["columns"]],
            "Location": location,
            **INPUT_OUTPUT,
            "SerdeInfo": PARQUET_SERDE,
        },
        "PartitionKeys": [{"Name": c, "Type": t} for c, t in spec["partitions"]],
    }
    try:
        glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"CREATE: {nome}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
        print(f"UPDATE: {nome}")


def _aguardar(athena, query_id: str) -> None:
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
    if not settings.bucket_gold:
        raise ValueError("BUCKET_GOLD não configurado")

    session = boto3.Session(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_default_region,
    )
    glue = session.client("glue")
    athena = session.client("athena")
    output = f"s3://{settings.bucket_gold}/athena-results/"

    for nome, spec in TABELAS.items():
        _criar_ou_atualizar(glue, nome, settings.bucket_gold, spec)

    for nome in TABELAS:
        sql = f"MSCK REPAIR TABLE {DATABASE}.{nome}"
        resp = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": output},
        )
        _aguardar(athena, resp["QueryExecutionId"])
        print(f"REPAIR: {nome}")


if __name__ == "__main__":
    main()
