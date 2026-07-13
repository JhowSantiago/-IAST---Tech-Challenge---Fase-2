"""Publica scripts Glue, biblioteca src/ e staging no S3."""

from __future__ import annotations

import sys
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402

GLUE_PREFIX = "glue"
SCRIPTS = {
    "etl-bronze-batch.py": ROOT / "src" / "bronze" / "etl-bronze-batch.py",
    "etl-silver.py": ROOT / "src" / "silver" / "etl-silver.py",
    "etl-silver-integracao.py": ROOT / "src" / "silver" / "etl-silver-integracao.py",
    "etl-gold.py": ROOT / "src" / "gold" / "etl-gold.py",
}


def _zip_src() -> bytes:
    buffer = BytesIO()
    src_dir = ROOT / "src"
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in src_dir.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                zf.write(path, arcname=str(path.relative_to(ROOT)))
    buffer.seek(0)
    return buffer.read()


def _upload_bytes(s3, bucket: str, key: str, data: bytes) -> str:
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    uri = f"s3://{bucket}/{key}"
    print(f"UPLOAD {uri}")
    return uri


def _upload_file(s3, bucket: str, key: str, path: Path) -> str:
    s3.upload_file(str(path), bucket, key)
    uri = f"s3://{bucket}/{key}"
    print(f"UPLOAD {uri}")
    return uri


def publicar(*, incluir_staging: bool = True) -> dict[str, str]:
    load_dotenv(ROOT / ".env")
    settings = get_settings()
    if not settings.bucket_bronze:
        raise ValueError("BUCKET_BRONZE não configurado")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    bucket = settings.bucket_bronze
    artefatos: dict[str, str] = {}

    artefatos["libs"] = _upload_bytes(
        s3, bucket, f"{GLUE_PREFIX}/libs/pipeline_src.zip", _zip_src()
    )
    for nome, path in SCRIPTS.items():
        artefatos[nome] = _upload_file(s3, bucket, f"{GLUE_PREFIX}/scripts/{nome}", path)

    s3.put_object(Bucket=bucket, Key=f"{GLUE_PREFIX}/temp/.keep", Body=b"")
    print(f"UPLOAD s3://{bucket}/{GLUE_PREFIX}/temp/.keep")

    if incluir_staging:
        staging = ROOT / "data" / "staging"
        for entidade in ENTIDADES_BATCH:
            origem = staging / f"{entidade}.parquet"
            if not origem.exists():
                raise FileNotFoundError(f"Staging ausente: {origem}")
            artefatos[f"staging_{entidade}"] = _upload_file(
                s3, bucket, f"staging/{entidade}/{entidade}.parquet", origem
            )

    return artefatos


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Publica assets Glue no S3")
    parser.add_argument("--sem-staging", action="store_true")
    args = parser.parse_args()
    artefatos = publicar(incluir_staging=not args.sem_staging)
    print("\nPublicação concluída:")
    for k, v in artefatos.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
