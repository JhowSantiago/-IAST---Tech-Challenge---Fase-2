"""Carrega entidades da Bronze para a camada Silver no S3."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.silver.integracao_pandas import processar_integracao_silver  # noqa: E402
from src.silver.processar_silver import processar_silver_batch  # noqa: E402
from src.silver.transformacoes_pandas import ENTIDADES_SILVER_BATCH  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Carga Bronze → Silver")
    parser.add_argument("--entidade", choices=ENTIDADES_SILVER_BATCH + ["integracao"])
    parser.add_argument("--todas", action="store_true")
    parser.add_argument("--integracao", action="store_true")
    args = parser.parse_args()

    if not any([args.entidade, args.todas, args.integracao]):
        parser.error("Informe --entidade, --todas ou --integracao")

    if not settings.bucket_bronze or not settings.bucket_silver:
        raise ValueError("BUCKET_BRONZE e BUCKET_SILVER devem estar no .env")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    resultados = []
    if args.todas:
        resultados = processar_silver_batch(
            settings.bucket_bronze,
            settings.bucket_silver,
            s3,
        )
        resultados.append(
            processar_integracao_silver(
                settings.bucket_bronze,
                settings.bucket_silver,
                s3,
            )
        )
    elif args.integracao or args.entidade == "integracao":
        resultados.append(
            processar_integracao_silver(
                settings.bucket_bronze,
                settings.bucket_silver,
                s3,
            )
        )
    else:
        from src.silver.processar_silver import processar_entidade_silver

        resultados.append(
            processar_entidade_silver(
                args.entidade,
                settings.bucket_bronze,
                settings.bucket_silver,
                s3,
            )
        )

    print(json.dumps(resultados, indent=2, default=str))


if __name__ == "__main__":
    main()
