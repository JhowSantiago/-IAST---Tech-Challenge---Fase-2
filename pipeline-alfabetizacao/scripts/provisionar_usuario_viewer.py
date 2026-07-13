"""Cria usuário IAM somente leitura para validação da entrega."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
USER_NAME = "alfabetizacao-viewer"
POLICY_NAME = "alfabetizacao-viewer-readonly"
DATABASE_NAME = "datalake_alfabetizacao"


def main() -> None:
    load_dotenv(ROOT / ".env")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    bronze = os.environ["BUCKET_BRONZE"]
    silver = os.environ["BUCKET_SILVER"]
    gold = os.environ["BUCKET_GOLD"]

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region,
    )
    iam = session.client("iam")
    sts = session.client("sts")
    account = sts.get_caller_identity()["Account"]

    template = (ROOT / "infra/aws/iam-viewer-policy.json").read_text(encoding="utf-8")
    policy_doc = (
        template.replace("${BUCKET_BRONZE}", bronze)
        .replace("${BUCKET_SILVER}", silver)
        .replace("${BUCKET_GOLD}", gold)
        .replace("${AWS_REGION}", region)
        .replace("${AWS_ACCOUNT_ID}", account)
        .replace("${DATABASE_NAME}", DATABASE_NAME)
    )

    try:
        iam.get_user(UserName=USER_NAME)
        print(f"Usuário {USER_NAME} já existe")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_user(
            UserName=USER_NAME,
            Tags=[
                {"Key": "project", "Value": "tech-challenge-fase2"},
                {"Key": "role", "Value": "viewer"},
            ],
        )
        print(f"Usuário {USER_NAME} criado")

    iam.put_user_policy(UserName=USER_NAME, PolicyName=POLICY_NAME, PolicyDocument=policy_doc)
    print(f"Política {POLICY_NAME} aplicada")

    cred_path = ROOT / ".env.viewer"
    if cred_path.exists():
        print(f"Credenciais existentes em {cred_path} (não recriadas)")
        print("Para nova access key: aws iam create-access-key --user-name", USER_NAME)
        return

    key = iam.create_access_key(UserName=USER_NAME)["AccessKey"]
    conteudo = (
        f"# Usuário somente leitura — NÃO commitar\n"
        f"VIEWER_AWS_ACCESS_KEY_ID={key['AccessKeyId']}\n"
        f"VIEWER_AWS_SECRET_ACCESS_KEY={key['SecretAccessKey']}\n"
        f"AWS_DEFAULT_REGION={region}\n"
        f"BUCKET_BRONZE={bronze}\n"
        f"BUCKET_SILVER={silver}\n"
        f"BUCKET_GOLD={gold}\n"
    )
    cred_path.write_text(conteudo, encoding="utf-8")
    example = ROOT / ".env.viewer.example"
    example.write_text(
        "# Copie para .env.viewer após provisionar_usuario_viewer.py\n"
        "VIEWER_AWS_ACCESS_KEY_ID=\n"
        "VIEWER_AWS_SECRET_ACCESS_KEY=\n"
        f"AWS_DEFAULT_REGION={region}\n",
        encoding="utf-8",
    )
    print(f"Credenciais viewer gravadas em {cred_path}")
    print("Permissões: S3/Glue/Athena LEITURA; sem Put/Delete/StartJobRun")


if __name__ == "__main__":
    main()
