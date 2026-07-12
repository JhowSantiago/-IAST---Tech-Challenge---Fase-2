"""Sincroniza Parquet de staging local para o bucket bronze no S3."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = ROOT / "data" / "staging"


def main() -> None:
    load_dotenv(ROOT / ".env")
    bucket = os.environ["BUCKET_BRONZE"]
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    if not STAGING_DIR.exists():
        raise FileNotFoundError(f"Diretório de staging ausente: {STAGING_DIR}")

    for arquivo in STAGING_DIR.glob("*.parquet"):
        key = f"staging/{arquivo.name}"
        s3.upload_file(str(arquivo), bucket, key)
        print(f"Enviado s3://{bucket}/{key}")

    print("Upload de staging concluído.")


if __name__ == "__main__":
    main()
