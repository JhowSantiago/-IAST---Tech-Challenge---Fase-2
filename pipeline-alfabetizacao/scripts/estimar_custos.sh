#!/usr/bin/env bash
# Consulta AWS Cost Explorer filtrando tag project=tech-challenge-fase2.
# Requer: AWS CLI configurado e permissão ce:GetCostAndUsage.
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
PROJECT_TAG="${FINOPS_PROJECT_TAG:-tech-challenge-fase2}"

# Período: mês corrente (UTC)
START="$(date -u +%Y-%m-01)"
END="$(date -u +%Y-%m-%d)"

echo "=== Estimativa de custos AWS ==="
echo "Projeto : $PROJECT_TAG"
echo "Região  : $REGION"
echo "Período : $START → $END"
echo ""

if ! aws ce get-cost-and-usage \
  --region us-east-1 \
  --time-period "Start=$START,End=$END" \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --filter "{\"Tags\":{\"Key\":\"project\",\"Values\":[\"$PROJECT_TAG\"]}}" \
  --output table 2>/dev/null; then
  echo "Cost Explorer indisponível ou tag ainda sem dados."
  echo ""
  echo "Estimativa manual (dev) — ver docs/finops.md:"
  echo "  S3 (~5 GB)     ~\$0,12"
  echo "  Glue jobs      ~\$15"
  echo "  Athena scan    ~\$0,05"
  echo "  Total          ~\$15–20/mês"
  exit 0
fi

echo ""
echo "Detalhe por serviço (sem filtro de tag, últimos 30 dias):"

START30="$(date -u -d '30 days ago' +%Y-%m-%d 2>/dev/null || date -u -v-30d +%Y-%m-%d)"
aws ce get-cost-and-usage \
  --region us-east-1 \
  --time-period "Start=$START30,End=$END" \
  --granularity MONTHLY \
  --metrics "UnblendedCost" \
  --group-by "Type=DIMENSION,Key=SERVICE" \
  --filter "{\"Dimensions\":{\"Key\":\"SERVICE\",\"Values\":[\"Amazon Simple Storage Service\",\"AWS Glue\",\"Amazon Athena\"]}}" \
  --output table 2>/dev/null || true
