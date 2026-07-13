"""Cria ou atualiza jobs AWS Glue da pipeline ICA."""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402
from src.gold.agregacoes_pandas import VISOES_GOLD  # noqa: E402

ROLE_NAME = "glue-alfabetizacao-role"
GLUE_VERSION = "4.0"
WORKER_TYPE = "G.1X"
NUM_WORKERS = 2
TIMEOUT_MIN = 60


def _job_base(
    bucket_bronze: str,
    libs_uri: str,
    temp_dir: str,
) -> dict:
    return {
        "GlueVersion": GLUE_VERSION,
        "Role": ROLE_NAME,
        "ExecutionProperty": {"MaxConcurrentRuns": 2},
        "Command": {
            "Name": "glueetl",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{bucket_bronze}/glue/sparkHistory/",
            "--extra-py-files": libs_uri,
            "--TempDir": temp_dir,
        },
        "WorkerType": WORKER_TYPE,
        "NumberOfWorkers": NUM_WORKERS,
        "Timeout": TIMEOUT_MIN,
        "MaxRetries": 0,
    }


def _definicoes_jobs(bronze: str, silver: str, gold: str, scripts: str, libs: str) -> dict[str, dict]:
    temp = f"s3://{bronze}/glue/temp/"
    base = _job_base(bronze, libs, temp)
    jobs: dict[str, dict] = {}

    for entidade in ENTIDADES_BATCH:
        nome = f"etl-bronze-batch-{entidade}"
        jobs[nome] = {
            **base,
            "Name": nome,
            "Description": f"Bronze batch — {entidade}",
            "Command": {
                **base["Command"],
                "ScriptLocation": f"{scripts}etl-bronze-batch.py",
            },
            "DefaultArguments": {
                **base["DefaultArguments"],
                "--ENTIDADE": entidade,
                "--BUCKET_BRONZE": bronze,
                "--SOURCE_PATH": f"s3://{bronze}/staging/{entidade}/",
            },
        }

    for entidade in ENTIDADES_BATCH:
        nome = f"etl-silver-{entidade}"
        jobs[nome] = {
            **base,
            "Name": nome,
            "Description": f"Silver — {entidade}",
            "Command": {
                **base["Command"],
                "ScriptLocation": f"{scripts}etl-silver.py",
            },
            "DefaultArguments": {
                **base["DefaultArguments"],
                "--ENTIDADE": entidade,
                "--BUCKET_BRONZE": bronze,
                "--BUCKET_SILVER": silver,
                "--BRONZE_PATH": f"s3://{bronze}/bronze/batch/{entidade}/",
            },
        }

    jobs["etl-silver-integracao"] = {
        **base,
        "Name": "etl-silver-integracao",
        "Description": "Silver integração batch + streaming",
        "Command": {
            **base["Command"],
            "ScriptLocation": f"{scripts}etl-silver-integracao.py",
        },
        "DefaultArguments": {
            **base["DefaultArguments"],
            "--BUCKET_BRONZE": bronze,
            "--BUCKET_SILVER": silver,
            "--SILVER_PREFIX": f"s3://{silver}/silver/",
        },
    }

    for visao in VISOES_GOLD:
        nome = f"etl-gold-{visao}"
        jobs[nome] = {
            **base,
            "Name": nome,
            "Description": f"Gold — {visao}",
            "Command": {
                **base["Command"],
                "ScriptLocation": f"{scripts}etl-gold.py",
            },
            "DefaultArguments": {
                **base["DefaultArguments"],
                "--VISAO": visao,
                "--BUCKET_SILVER": silver,
                "--BUCKET_GOLD": gold,
                "--SILVER_PATH": f"s3://{silver}/silver/municipio_indicador_completo/",
            },
        }

    return jobs


def provisionar() -> list[str]:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    bronze = settings.bucket_bronze
    silver = settings.bucket_silver
    gold = settings.bucket_gold

    iam = boto3.client(
        "iam",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    glue = boto3.client(
        "glue",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    scripts = f"s3://{bronze}/glue/scripts/"
    libs = f"s3://{bronze}/glue/libs/pipeline_src.zip"

    jobs = _definicoes_jobs(bronze, silver, gold, scripts, libs)
    criados: list[str] = []

    for nome, job_input in jobs.items():
        job_input = {**job_input, "Role": role_arn}
        update_payload = {k: v for k, v in job_input.items() if k != "Name"}
        try:
            glue.get_job(JobName=nome)
            glue.update_job(JobName=nome, JobUpdate=update_payload)
            print(f"UPDATE job {nome}")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "EntityNotFoundException":
                raise
            glue.create_job(**job_input)
            print(f"CREATE job {nome}")
        criados.append(nome)

    return criados


def main() -> None:
    jobs = provisionar()
    print(f"\n{len(jobs)} jobs Glue provisionados.")


if __name__ == "__main__":
    main()
