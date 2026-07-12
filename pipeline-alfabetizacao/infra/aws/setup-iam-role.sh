#!/usr/bin/env bash
# Cria a IAM Role glue-alfabetizacao-role com least privilege para Glue + S3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ROLE_NAME="glue-alfabetizacao-role"
POLICY_NAME="glue-alfabetizacao-s3-glue-policy"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

REGION="${AWS_DEFAULT_REGION:-us-east-2}"
BUCKET_BRONZE="${BUCKET_BRONZE:?Defina BUCKET_BRONZE no .env}"
BUCKET_SILVER="${BUCKET_SILVER:?Defina BUCKET_SILVER no .env}"
BUCKET_GOLD="${BUCKET_GOLD:?Defina BUCKET_GOLD no .env}"

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

render_policy() {
  sed \
    -e "s|\${BUCKET_BRONZE}|${BUCKET_BRONZE}|g" \
    -e "s|\${BUCKET_SILVER}|${BUCKET_SILVER}|g" \
    -e "s|\${BUCKET_GOLD}|${BUCKET_GOLD}|g" \
    -e "s|\${AWS_REGION}|${REGION}|g" \
    -e "s|\${AWS_ACCOUNT_ID}|${AWS_ACCOUNT_ID}|g" \
    "$SCRIPT_DIR/iam-glue-role.json"
}

if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Role ${ROLE_NAME} já existe — atualizando policy inline."
else
  echo "Criando role ${ROLE_NAME}..."
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "file://${SCRIPT_DIR}/iam-glue-trust-policy.json" \
    --description "Role para jobs e crawlers Glue da pipeline ICA"
  aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
fi

POLICY_FILE="$(mktemp)"
render_policy > "$POLICY_FILE"

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document "file://${POLICY_FILE}"

rm -f "$POLICY_FILE"

ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query Role.Arn --output text)"
echo "Role criada/atualizada: ${ROLE_ARN}"
