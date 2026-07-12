# Glue Data Catalog — Pipeline ICA

Configuração do metastore AWS Glue para consultas Athena sobre os dados medalhão.

## Database

| Campo | Valor |
|-------|-------|
| Nome | `datalake_alfabetizacao` |
| Região | `us-east-2` (conforme `.env`) |

## Crawlers

| Crawler | Target S3 | Quando executar |
|---------|-----------|-----------------|
| `crawler-bronze-batch` | `s3://{BUCKET_BRONZE}/bronze/batch/` | Após cada ingestão batch (Fase 04) |
| `crawler-bronze-streaming` | `s3://{BUCKET_BRONZE}/bronze/streaming/` | Após eventos streaming (Fase 05) |

Provisionamento:

```bash
bash scripts/registrar_crawlers.sh
# ou, em ambientes sem bash:
python scripts/provisionar_iam_glue.py
```

Execução manual de um crawler:

```bash
aws glue start-crawler --name crawler-bronze-batch --region us-east-2
aws glue get-crawler --name crawler-bronze-batch --region us-east-2
```

## Crawler vs DDL manual

| Camada | Estratégia | Motivo |
|--------|------------|--------|
| **Bronze** | Crawler Glue (inferência de schema) | Dados brutos; schema pode variar na origem; alinhado à aula 3.3 |
| **Silver** | DDL manual (`sql/athena/silver_ddl.sql` — Fase 06) | Schema controlado pós-transformação e validação DQ |
| **Gold** | DDL manual (`sql/athena/gold_ddl.sql` — Fase 07) | Visões analíticas com colunas derivadas (`gap_meta`, `atingiu_meta`) |

### Por que não usar crawler na Silver/Gold?

1. Colunas derivadas e renomeações (`sigla` → `sigla_uf`) não são inferidas corretamente.
2. Particionamento e tipos precisam ser explícitos para performance no Athena.
3. O contrato de dados está definido em `src/dq/schemas.py`.

## DDL Bronze

Templates em `sql/athena/bronze_ddl.sql` para as 6 entidades batch.

Antes de executar no Athena:

1. Substituir `{BUCKET_BRONZE}` pelo nome real do bucket
2. Executar `CREATE EXTERNAL TABLE` por entidade
3. Após ingestão: `MSCK REPAIR TABLE bronze_{entidade};`

## Athena Workgroup (recomendação)

| Campo | Valor sugerido |
|-------|----------------|
| Nome | `ica-analytics` |
| Resultados | `s3://{BUCKET_SILVER}/athena-results/` |

Criar workgroup evita custos inesperados e centraliza resultados de consulta.

## Verificação

```bash
aws glue get-database --name datalake_alfabetizacao --region us-east-2
aws glue list-crawlers --region us-east-2
```

Após ingestão e `MSCK REPAIR`:

```sql
SELECT COUNT(*) FROM datalake_alfabetizacao.bronze_meta_municipio;
```
