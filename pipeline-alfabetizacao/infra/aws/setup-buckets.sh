#!/usr/bin/env bash
# Provisiona prefixos medalhão e tags FinOps nos buckets S3.
# Pré-requisito: buckets já criados e variáveis no .env preenchidas.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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

create_prefix() {
  local bucket="$1"
  local prefix="$2"
  echo "Criando s3://${bucket}/${prefix}.keep"
  printf '' | aws s3 cp - "s3://${bucket}/${prefix}.keep" --region "$REGION"
}

tag_bucket() {
  local bucket="$1"
  local layer="$2"
  aws s3api put-bucket-tagging \
    --bucket "$bucket" \
    --tagging "TagSet=[{Key=project,Value=tech-challenge-fase2},{Key=environment,Value=dev},{Key=finops,Value=tracked},{Key=layer,Value=${layer}}]" \
    --region "$REGION"
}

verify_bucket() {
  local bucket="$1"
  aws s3api head-bucket --bucket "$bucket" --region "$REGION"
}

echo "==> Verificando buckets..."
verify_bucket "$BUCKET_BRONZE"
verify_bucket "$BUCKET_SILVER"
verify_bucket "$BUCKET_GOLD"

echo "==> Criando prefixos bronze..."
create_prefix "$BUCKET_BRONZE" "bronze/batch/"
create_prefix "$BUCKET_BRONZE" "bronze/streaming/"

echo "==> Criando prefixos silver..."
create_prefix "$BUCKET_SILVER" "silver/"
create_prefix "$BUCKET_SILVER" "quarentena/"

echo "==> Criando prefixos gold..."
create_prefix "$BUCKET_GOLD" "gold/"

echo "==> Aplicando tags FinOps..."
tag_bucket "$BUCKET_BRONZE" "bronze"
tag_bucket "$BUCKET_SILVER" "silver"
tag_bucket "$BUCKET_GOLD" "gold"

echo "==> Estrutura medalhão configurada com sucesso."
aws s3 ls "s3://${BUCKET_BRONZE}/bronze/" --region "$REGION"
