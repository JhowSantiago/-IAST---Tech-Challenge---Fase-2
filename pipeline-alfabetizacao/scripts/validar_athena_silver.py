"""Valida contagens das tabelas Silver no Athena."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402

QUERIES = {
    "silver_uf": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.silver_uf",
    "silver_meta_municipio": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.silver_meta_municipio",
    "silver_integrado": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.silver_municipio_indicador_completo",
    "streaming": (
        "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.silver_municipio_indicador_completo "
        "WHERE _source_type = 'streaming'"
    ),
}


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    athena = boto3.client(
        "athena",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    output = f"s3://{settings.bucket_silver}/athena-results/"

    for nome, sql in QUERIES.items():
        qid = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": "datalake_alfabetizacao"},
            ResultConfiguration={"OutputLocation": output},
        )["QueryExecutionId"]
        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if status["State"] != "SUCCEEDED":
            raise RuntimeError(f"{nome}: {status.get('StateChangeReason')}")
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        print(f"{nome}: {rows[1]['Data'][0]['VarCharValue']}")


if __name__ == "__main__":
    main()
