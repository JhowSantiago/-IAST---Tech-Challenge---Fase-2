#!/usr/bin/env bash
# Registra database e crawlers Glue para a camada bronze.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

REGION="${AWS_DEFAULT_REGION:-us-east-2}"
BUCKET_BRONZE="${BUCKET_BRONZE:?Defina BUCKET_BRONZE no .env}"
DATABASE_NAME="datalake_alfabetizacao"
ROLE_NAME="glue-alfabetizacao-role"

ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query Role.Arn --output text)"

create_database() {
  if aws glue get-database --name "$DATABASE_NAME" --region "$REGION" >/dev/null 2>&1; then
    echo "Database ${DATABASE_NAME} já existe."
    return
  fi

  aws glue create-database \
    --region "$REGION" \
    --database-input "{
      \"Name\": \"${DATABASE_NAME}\",
      \"Description\": \"Data Catalog da pipeline ICA - Tech Challenge Fase 2\"
    }"
  echo "Database ${DATABASE_NAME} criado."
}

create_crawler() {
  local crawler_name="$1"
  local s3_path="$2"

  if aws glue get-crawler --name "$crawler_name" --region "$REGION" >/dev/null 2>&1; then
    echo "Crawler ${crawler_name} já existe — atualizando target."
    aws glue update-crawler \
      --region "$REGION" \
      --name "$crawler_name" \
      --role "$ROLE_ARN" \
      --database-name "$DATABASE_NAME" \
      --targets "{\"S3Targets\":[{\"Path\":\"${s3_path}\"}]}" \
      --schema-change-policy "{\"UpdateBehavior\":\"UPDATE_IN_DATABASE\",\"DeleteBehavior\":\"LOG\"}"
    return
  fi

  aws glue create-crawler \
    --region "$REGION" \
    --name "$crawler_name" \
    --role "$ROLE_ARN" \
    --database-name "$DATABASE_NAME" \
    --targets "{\"S3Targets\":[{\"Path\":\"${s3_path}\"}]}" \
    --schema-change-policy "{\"UpdateBehavior\":\"UPDATE_IN_DATABASE\",\"DeleteBehavior\":\"LOG\"}" \
    --description "Crawler bronze - ${crawler_name}"
  echo "Crawler ${crawler_name} criado."
}

ENTIDADES_BATCH=(uf municipio meta_brasil meta_uf meta_municipio alunos)

create_database

for entidade in "${ENTIDADES_BATCH[@]}"; do
  create_crawler "crawler-bronze-${entidade}" "s3://${BUCKET_BRONZE}/bronze/batch/${entidade}/"
done

create_crawler "crawler-bronze-streaming" "s3://${BUCKET_BRONZE}/bronze/streaming/indicador_alfabetizacao/"

# Remove crawler legado que inferia tabela única "batch" na raiz batch/
if aws glue get-crawler --name "crawler-bronze-batch" --region "$REGION" >/dev/null 2>&1; then
  aws glue delete-crawler --name "crawler-bronze-batch" --region "$REGION"
  echo "Crawler legado crawler-bronze-batch removido."
fi

echo "Glue Data Catalog configurado."
aws glue get-database --name "$DATABASE_NAME" --region "$REGION" --query Database.Name --output text
