# Deploy AWS — Pipeline Glue

Guia para publicar e executar a pipeline na AWS (além dos scripts locais pandas).

## Pré-requisitos

- `.env` com credenciais admin e buckets configurados
- Role `glue-alfabetizacao-role` (via `provisionar_iam_glue.py`)
- Dados em `data/staging/` para carga Bronze via Glue

## Deploy em um comando

```powershell
cd pipeline-alfabetizacao
python scripts/deploy_aws.py --executar-smoke
```

Etapas do `deploy_aws.py`:

1. IAM + crawlers Glue
2. Publica scripts e `pipeline_src.zip` em `s3://{BUCKET_BRONZE}/glue/`
3. Cria/atualiza **16 jobs Glue** (Bronze, Silver, Gold)
4. Usuário viewer somente leitura
5. Registra tabelas Bronze/Silver/Gold no Athena
6. (opcional) smoke test na AWS

## Jobs Glue

| Camada | Jobs |
|--------|------|
| Bronze | `etl-bronze-batch-{uf,municipio,meta_*,alunos}` |
| Silver | `etl-silver-{entidade}` + `etl-silver-integracao` |
| Gold | `etl-gold-{indicador_municipio,meta_vs_resultado,evolucao_temporal}` |

Configuração: Glue 4.0, worker **G.1X**, **2 workers** (FinOps).

## Executar na AWS

```powershell
# Smoke: UF bronze → silver → gold parcial (~5 min)
python scripts/executar_pipeline_aws.py --modo smoke

# Só Gold (usa Silver já carregada)
python scripts/executar_pipeline_aws.py --modo gold

# Pipeline completa (Bronze→Silver→Gold, ~30+ min com alunos)
python scripts/executar_pipeline_aws.py --modo completo

# Job individual
python scripts/executar_pipeline_aws.py --job etl-gold-meta_vs_resultado
```

## Usuário viewer (validação da entrega)

Usuário IAM **`alfabetizacao-viewer`** — somente leitura:

- S3: `GetObject`, `ListBucket` nos 3 buckets
- Glue: consulta catálogo e status de jobs
- Athena: executar queries de leitura
- **Negado:** `PutObject`, `DeleteObject`, `StartJobRun`, alterações IAM

```powershell
python scripts/provisionar_usuario_viewer.py
```

Credenciais geradas em `.env.viewer` (gitignored). Template: `.env.viewer.example`.

### Validar com o viewer no Athena

1. Configure o perfil AWS com as credenciais do viewer
2. Abra Athena → database `datalake_alfabetizacao`
3. Execute: `SELECT * FROM gold_indicador_municipio LIMIT 10;`

## Alternativa local (Windows)

Para desenvolvimento, use os scripts pandas (mais rápidos):

```powershell
python scripts/carregar_bronze_batch.py --todas
python scripts/carregar_silver.py --todas
python scripts/carregar_gold.py --todas
```

Ver `infra/aws/glue-workflow.json` → seção `LocalAlternative`.

## Validação pós-deploy

```powershell
python tests/validation/validar_pipeline.py
python -m pytest tests/validation/test_fase_aws_glue.py -v
```
