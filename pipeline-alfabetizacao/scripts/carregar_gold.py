"""Carrega visões analíticas da Silver para a camada Gold no S3."""

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
from src.gold.agregacoes_pandas import VISOES_GOLD  # noqa: E402
from src.gold.processar_gold import processar_gold_completo, processar_visao_gold  # noqa: E402

logger = get_logger(__name__)


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Carga Silver → Gold")
    parser.add_argument("--visao", choices=VISOES_GOLD)
    parser.add_argument("--todas", action="store_true")
    args = parser.parse_args()

    if not args.visao and not args.todas:
        parser.error("Informe --visao ou --todas")

    if not settings.bucket_silver or not settings.bucket_gold:
        raise ValueError("BUCKET_SILVER e BUCKET_GOLD devem estar no .env")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    if args.todas:
        resultados = processar_gold_completo(
            settings.bucket_silver,
            settings.bucket_gold,
            s3,
        )
    else:
        resultados = [
            processar_visao_gold(
                args.visao,
                settings.bucket_silver,
                settings.bucket_gold,
                s3,
            )
        ]

    print(json.dumps(resultados, indent=2, default=str))


if __name__ == "__main__":
    main()
