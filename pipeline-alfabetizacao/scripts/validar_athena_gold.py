"""Valida contagens e queries da camada Gold no Athena."""

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
    "gold_indicador_municipio": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.gold_indicador_municipio",
    "gold_meta_vs_resultado": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.gold_meta_vs_resultado",
    "gold_evolucao_temporal": "SELECT COUNT(*) AS n FROM datalake_alfabetizacao.gold_evolucao_temporal",
    "top_gap_negativo": (
        "SELECT nome, sigla_uf, gap_meta FROM datalake_alfabetizacao.gold_indicador_municipio "
        "WHERE atingiu_meta = false ORDER BY gap_meta ASC LIMIT 3"
    ),
}


def _executar(athena, sql: str, output: str) -> list[list[str]]:
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
        raise RuntimeError(status.get("StateChangeReason"))
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [[d.get("VarCharValue", "") for d in r["Data"]] for r in rows]


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    athena = boto3.client(
        "athena",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    output = f"s3://{settings.bucket_gold}/athena-results/"

    for nome, sql in QUERIES.items():
        rows = _executar(athena, sql, output)
        print(f"{nome}: {rows}")


if __name__ == "__main__":
    main()
