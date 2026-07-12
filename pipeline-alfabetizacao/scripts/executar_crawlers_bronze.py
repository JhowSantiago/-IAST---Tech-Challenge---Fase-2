"""Registra e executa crawlers bronze (um por entidade batch)."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH  # noqa: E402

DATABASE_NAME = "datalake_alfabetizacao"
TABELA_LEGADA = "batch"
POLL_SECONDS = 15
MAX_WAIT_SECONDS = 600


def _aguardar_crawler(glue, nome: str) -> None:
    inicio = time.time()
    while True:
        estado = glue.get_crawler(Name=nome)["Crawler"]["State"]
        if estado == "READY":
            return
        if time.time() - inicio > MAX_WAIT_SECONDS:
            raise TimeoutError(f"Crawler {nome} não ficou READY em {MAX_WAIT_SECONDS}s (estado={estado})")
        time.sleep(POLL_SECONDS)


def main() -> None:
    load_dotenv(ROOT / ".env")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "provisionar_iam_glue.py")],
        check=True,
    )

    session = boto3.Session()
    glue = session.client("glue")

    for entidade in ENTIDADES_BATCH:
        nome = f"crawler-bronze-{entidade}"
        _aguardar_crawler(glue, nome)
        try:
            glue.start_crawler(Name=nome)
            print(f"Crawler {nome} iniciado")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "CrawlerRunningException":
                print(f"Crawler {nome} já em execução")
            else:
                raise

    for entidade in ENTIDADES_BATCH:
        nome = f"crawler-bronze-{entidade}"
        _aguardar_crawler(glue, nome)
        print(f"Crawler {nome} concluído")

    tabelas = glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"]
    nomes = sorted(t["Name"] for t in tabelas)
    print("TABELAS", nomes)

    if TABELA_LEGADA in nomes:
        print(f"AVISO: tabela legada '{TABELA_LEGADA}' ainda presente — remova manualmente se necessário")


if __name__ == "__main__":
    main()
