"""Inicializa prefixos S3 e tags FinOps nos buckets medalhao."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    load_dotenv(ROOT / ".env")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    bronze = os.environ["BUCKET_BRONZE"]
    silver = os.environ["BUCKET_SILVER"]
    gold = os.environ["BUCKET_GOLD"]

    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    prefixes = {
        bronze: ["bronze/batch/", "bronze/streaming/"],
        silver: ["silver/", "quarentena/"],
        gold: ["gold/"],
    }
    layers = {bronze: "bronze", silver: "silver", gold: "gold"}

    for bucket, paths in prefixes.items():
        s3.head_bucket(Bucket=bucket)
        for prefix in paths:
            key = f"{prefix}.keep"
            s3.put_object(Bucket=bucket, Key=key, Body=b"")
            print(f"Prefixo criado: s3://{bucket}/{key}")

        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                "TagSet": [
                    {"Key": "project", "Value": "tech-challenge-fase2"},
                    {"Key": "environment", "Value": "dev"},
                    {"Key": "finops", "Value": "tracked"},
                    {"Key": "layer", "Value": layers[bucket]},
                ]
            },
        )
        print(f"Tags aplicadas em {bucket}")

    listing = s3.list_objects_v2(Bucket=bronze, Prefix="bronze/", Delimiter="/")
    print("Prefixos bronze:", [p["Prefix"] for p in listing.get("CommonPrefixes", [])])


if __name__ == "__main__":
    main()
