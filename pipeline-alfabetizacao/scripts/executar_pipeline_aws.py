"""Executa pipeline no AWS Glue e aguarda conclusão."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402
from src.gold.agregacoes_pandas import VISOES_GOLD  # noqa: E402

SEQUENCIA_COMPLETA = (
    [f"etl-bronze-batch-{e}" for e in ENTIDADES_BATCH]
    + [f"etl-silver-{e}" for e in ENTIDADES_BATCH]
    + ["etl-silver-integracao"]
    + [f"etl-gold-{v}" for v in VISOES_GOLD]
)

SEQUENCIA_SMOKE = [
    "etl-bronze-batch-uf",
    "etl-silver-uf",
    "etl-gold-meta_vs_resultado",
]

SEQUENCIA_GOLD = [f"etl-gold-{v}" for v in VISOES_GOLD]


def _aguardar_job(glue, job_name: str, run_id: str, timeout_s: int = 3600) -> str:
    inicio = time.time()
    while time.time() - inicio < timeout_s:
        run = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        estado = run["JobRunState"]
        if estado in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            if estado != "SUCCEEDED":
                erro = run.get("ErrorMessage", estado)
                raise RuntimeError(f"Job {job_name} falhou: {erro}")
            return estado
        time.sleep(15)
    raise TimeoutError(f"Job {job_name} excedeu {timeout_s}s")


def executar(jobs: list[str], *, timeout_s: int = 3600) -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    glue = boto3.client(
        "glue",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    for job_name in jobs:
        try:
            glue.get_job(JobName=job_name)
        except glue.exceptions.EntityNotFoundException as exc:
            raise RuntimeError(f"Job não encontrado: {job_name}. Rode provisionar_jobs_glue.py") from exc

        print(f"\n>>> Iniciando {job_name}")
        run_id = glue.start_job_run(JobName=job_name)["JobRunId"]
        print(f"    RunId: {run_id}")
        _aguardar_job(glue, job_name, run_id, timeout_s=timeout_s)
        print(f"    {job_name} SUCCEEDED")


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa pipeline AWS Glue")
    parser.add_argument(
        "--modo",
        choices=("smoke", "gold", "completo"),
        default="smoke",
        help="smoke=uf+gold parcial; gold=só Gold; completo=pipeline inteira",
    )
    parser.add_argument("--job", help="Executa um único job pelo nome")
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()

    if args.job:
        jobs = [args.job]
    elif args.modo == "smoke":
        jobs = SEQUENCIA_SMOKE
    elif args.modo == "gold":
        jobs = SEQUENCIA_GOLD
    else:
        jobs = list(SEQUENCIA_COMPLETA)

    print(f"Modo: {args.modo or 'job único'} — {len(jobs)} job(s)")
    executar(jobs, timeout_s=args.timeout)
    print("\nPipeline AWS concluída com sucesso.")


if __name__ == "__main__":
    main()
