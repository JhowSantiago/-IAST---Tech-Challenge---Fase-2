"""Executa todos os crawlers bronze (batch + streaming) e configura Athena."""

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
WORKGROUP = "primary"
POLL_SECONDS = 10
MAX_WAIT_SECONDS = 600

CRAWLERS = [f"crawler-bronze-{e}" for e in ENTIDADES_BATCH] + ["crawler-bronze-streaming"]


def aguardar_crawler(glue, nome: str) -> None:
    inicio = time.time()
    while True:
        estado = glue.get_crawler(Name=nome)["Crawler"]["State"]
        if estado == "READY":
            return
        if time.time() - inicio > MAX_WAIT_SECONDS:
            raise TimeoutError(f"Crawler {nome} timeout (estado={estado})")
        time.sleep(POLL_SECONDS)


def configurar_athena(session, bucket_silver: str) -> None:
    s3 = session.client("s3")
    athena = session.client("athena")
    output = f"s3://{bucket_silver}/athena-results/"
    s3.put_object(Bucket=bucket_silver, Key="athena-results/.keep", Body=b"")

    try:
        wg = athena.get_work_group(WorkGroup=WORKGROUP)["WorkGroup"]
        cfg = wg.get("Configuration", {})
        result_cfg = cfg.get("ResultConfiguration", {})
        location = result_cfg.get("OutputLocation", "")
        if location == output:
            print(f"Athena workgroup {WORKGROUP} já configurado: {output}")
            return
    except ClientError:
        pass

    athena.update_work_group(
        WorkGroup=WORKGROUP,
        ConfigurationUpdates={
            "ResultConfigurationUpdates": {"OutputLocation": output},
            "EnforceWorkGroupConfiguration": True,
        },
    )
    print(f"Athena workgroup {WORKGROUP} -> {output}")


def main() -> None:
    load_dotenv(ROOT / ".env")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "provisionar_iam_glue.py")],
        check=True,
    )

    session = boto3.Session()
    glue = session.client("glue")
    silver = __import__("os").environ["BUCKET_SILVER"]

    configurar_athena(session, silver)

    for nome in CRAWLERS:
        aguardar_crawler(glue, nome)
        try:
            glue.start_crawler(Name=nome)
            print(f"Iniciado: {nome}")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "CrawlerRunningException":
                raise
            print(f"Já rodando: {nome}")

    for nome in CRAWLERS:
        aguardar_crawler(glue, nome)
        print(f"Concluído: {nome}")

    tabelas = sorted(t["Name"] for t in glue.get_tables(DatabaseName=DATABASE_NAME)["TableList"])
    print("TABELAS_GLUE", tabelas)


if __name__ == "__main__":
    main()
