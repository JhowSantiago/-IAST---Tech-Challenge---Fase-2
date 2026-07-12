#!/usr/bin/env bash
# Sincroniza arquivos Parquet de staging local para o bucket bronze.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

BUCKET_BRONZE="${BUCKET_BRONZE:?Defina BUCKET_BRONZE no .env}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
STAGING_DIR="$PROJECT_ROOT/data/staging"

aws s3 sync "$STAGING_DIR/" "s3://${BUCKET_BRONZE}/staging/" --region "$REGION"
echo "Staging sincronizado em s3://${BUCKET_BRONZE}/staging/"
