# FinOps — Pipeline Indicador Criança Alfabetizada

Documentação de decisões de otimização de custo, tags de rastreabilidade e estimativa mensal para o ambiente de desenvolvimento AWS.

## Princípios aplicados

1. **Medir antes de otimizar** — tags em buckets S3 e filtros no Cost Explorer.
2. **Formato colunar** — Parquet + SNAPPY em todas as camadas (Bronze, Silver, Gold).
3. **Particionamento** — reduz scan no Athena e no Glue.
4. **Right-sizing** — Glue workers G.1X com 2 workers no batch.
5. **Extração enxuta** — SELECT explícito no BigQuery (Base dos Dados), sem `SELECT *`.

## Decisões de otimização

| Decisão | Economia estimada | Trade-off |
|---------|-------------------|-----------|
| Parquet + SNAPPY vs CSV | ~80% storage + scan | Nenhum relevante |
| Particionamento `ano/mes/dia` (Bronze) e `mes/dia` (Silver/Gold) | ~90% scan Athena | Complexidade de paths |
| Glue G.1X, 2 workers | ~50% vs G.2X | Tempo de execução +30% |
| SELECT explícito no BigQuery | Evita scan desnecessário na extração | Manutenção das queries SQL |
| Kafka local vs Amazon MSK | ~$0 vs ~$150/mês | Não adequado para produção |
| Crawler só na Bronze | Evita re-inferência de schema | DDL manual Silver/Gold |
| DDL explícito Bronze (`registrar_tabelas_bronze_athena.py`) | Evita colunas duplicadas no Glue | Script de manutenção |
| Tags `project/layer/env/finops` | Visibilidade de custos por camada | Setup inicial |
| Deduplicação `id_aluno+ano` na Silver | Evita reprocessamento e joins incorretos | Lógica de negócio explícita |
| Quarentena DQ (Silver) | Falhas não contaminam camadas analíticas | Volume adicional em `quarentena/` |

## Tags FinOps (S3)

Aplicadas via `scripts/provisionar_buckets.py`:

| Tag | Valor | Propósito |
|-----|-------|-----------|
| `project` | `tech-challenge-fase2` | Agrupamento no Cost Explorer |
| `environment` | `dev` | Separar dev/prod |
| `finops` | `tracked` | Recursos monitorados |
| `layer` | `bronze` / `silver` / `gold` | Custo por camada medalhão |

## Estimativa mensal (ambiente dev)

Premissas: pipeline executada ~10×/dia, jobs Glue ~5 min, dados ~5 GB no S3, queries Athena ~10 GB scan/mês.

| Serviço | Cálculo | Custo estimado |
|---------|---------|----------------|
| S3 Standard (~5 GB) | 5 × $0,023/GB | ~$0,12 |
| S3 requests (PUT/GET) | uso moderado | ~$0,50 |
| AWS Glue (G.1X, 2 DPU, 10 runs/dia × 5 min) | ~300 DPU-min/dia | ~$15 |
| Amazon Athena (~10 GB scan) | 10 × $5/TB | ~$0,05 |
| Glue Data Catalog | < 1M objetos | ~$0 |
| **Total estimado** | | **~$15–20/mês** |

> Valores aproximados para `us-east-2` em jul/2026. Use `scripts/estimar_custos.sh` para consultar custos reais quando o Cost Explorer estiver habilitado (24–48 h após primeiro uso).

## Worker sizing (Glue)

| Job | Worker | Workers | Justificativa |
|-----|--------|---------|---------------|
| `etl-bronze-batch` | G.1X | 2 | Volume alto (alunos), mas transformação leve |
| `etl-silver` | G.1X | 2 | Joins moderados, Parquet columnar |
| `etl-gold` | G.1X | 2 | Agregações UF/município, dataset reduzido |

Upgrade para G.2X só se tempo de execução exceder SLA (> 30 min por job).

## Particionamento

| Camada | Entidade | Partições | Observação |
|--------|----------|-----------|------------|
| Bronze batch | meta_*, alunos | `ano/mes/dia` | `ano` = ano de avaliação INEP |
| Bronze batch | uf, municipio | `ano/mes/dia` | `ano` = ano de ingestão |
| Bronze streaming | indicador_alfabetizacao | `ano/mes/dia` | eventos Kafka simulados |
| Silver / Gold | todas | `mes/dia` | `ano` permanece coluna de negócio |

Colunas de partição **não** são gravadas no Parquet (evita `HIVE_INVALID_METADATA` no Athena).

## Validação FinOps

```powershell
# Tags nos buckets
python scripts/provisionar_buckets.py

# Validação end-to-end (inclui checagem de tags)
python tests/validation/validar_pipeline.py

# Cost Explorer (requer permissão ce:GetCostAndUsage)
bash scripts/estimar_custos.sh
```

## Referências

- Aulas 4.1 e 4.2 — FinOps para Dados (FIAP/POSTECH)
- [AWS Glue pricing](https://aws.amazon.com/glue/pricing/)
- [Amazon Athena pricing](https://aws.amazon.com/athena/pricing/)
