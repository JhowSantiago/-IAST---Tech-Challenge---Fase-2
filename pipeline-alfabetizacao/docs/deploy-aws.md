# Deploy AWS — Pipeline Glue

Este documento descreve como a pipeline foi publicada e executada na AWS, complementarmente aos scripts locais em pandas usados no desenvolvimento.

## Pré-requisitos utilizados

- Arquivo `.env` com credenciais administrativas e buckets configurados
- Role IAM `glue-alfabetizacao-role`, criada por `provisionar_iam_glue.py`
- Dados em `data/staging/` disponíveis para a carga Bronze via Glue

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

## Usuário viewer para validação da entrega

Foi criado o usuário IAM **`alfabetizacao-viewer`**, com permissão apenas de leitura:

- S3: `GetObject` e `ListBucket` nos três buckets
- Glue: consulta ao catálogo e visualização do status dos jobs
- Athena: execução de consultas de leitura

Não possui permissão para gravar ou apagar objetos no S3, iniciar jobs Glue nem alterar IAM.

```powershell
python scripts/provisionar_usuario_viewer.py
```

As credenciais ficam no arquivo local `.env.viewer` (fora do Git). O modelo está em `.env.viewer.example`.

### Consulta no Athena com o viewer

Com o perfil AWS configurado para o viewer, abra o Athena no database `datalake_alfabetizacao` e execute, por exemplo:

```sql
SELECT * FROM gold_indicador_municipio LIMIT 10;
```

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
