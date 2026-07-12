"""Provisiona IAM role e Glue Data Catalog da pipeline ICA."""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
ROLE_NAME = "glue-alfabetizacao-role"
POLICY_NAME = "glue-alfabetizacao-s3-glue-policy"
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
    sts = session.client("sts")
    iam = session.client("iam")
    glue = session.client("glue")
    account = sts.get_caller_identity()["Account"]

    trust = (ROOT / "infra/aws/iam-glue-trust-policy.json").read_text(encoding="utf-8")
    policy_template = (ROOT / "infra/aws/iam-glue-role.json").read_text(encoding="utf-8")
    policy = (
        policy_template.replace("${BUCKET_BRONZE}", bronze)
        .replace("${BUCKET_SILVER}", silver)
        .replace("${BUCKET_GOLD}", gold)
        .replace("${AWS_REGION}", region)
        .replace("${AWS_ACCOUNT_ID}", account)
    )

    try:
        iam.get_role(RoleName=ROLE_NAME)
        print(f"Role {ROLE_NAME} já existe")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=trust,
            Description="Role para jobs e crawlers Glue da pipeline ICA",
        )
        iam.attach_role_policy(
            RoleName=ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        )
        print(f"Role {ROLE_NAME} criada")

    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME, PolicyDocument=policy)
    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    print("ROLE_ARN", role_arn)

    try:
        glue.get_database(Name=DATABASE_NAME)
        print(f"Database {DATABASE_NAME} já existe")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "EntityNotFoundException":
            raise
        glue.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": "Data Catalog da pipeline ICA - Tech Challenge Fase 2",
            }
        )
        print(f"Database {DATABASE_NAME} criado")

    crawlers = {
        "crawler-bronze-batch": f"s3://{bronze}/bronze/batch/",
        "crawler-bronze-streaming": f"s3://{bronze}/bronze/streaming/",
    }
    for name, path in crawlers.items():
        try:
            glue.get_crawler(Name=name)
            glue.update_crawler(
                Name=name,
                Role=role_arn,
                DatabaseName=DATABASE_NAME,
                Targets={"S3Targets": [{"Path": path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
            )
            print(f"Crawler {name} atualizado")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "EntityNotFoundException":
                raise
            glue.create_crawler(
                Name=name,
                Role=role_arn,
                DatabaseName=DATABASE_NAME,
                Targets={"S3Targets": [{"Path": path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                Description=f"Crawler bronze - {name}",
            )
            print(f"Crawler {name} criado")

    print("GLUE_DB", glue.get_database(Name=DATABASE_NAME)["Database"]["Name"])


if __name__ == "__main__":
    main()
