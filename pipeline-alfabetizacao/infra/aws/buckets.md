# Buckets S3 — Arquitetura Medalhão

Documentação da camada de armazenamento da pipeline ICA (Tech Challenge Fase 2).

## Nomenclatura

| Camada | Variável `.env` | Bucket (exemplo) | Finalidade |
|--------|-----------------|------------------|------------|
| Bronze | `BUCKET_BRONZE` | `335596040535--tech-challenge-fase-2-bronze` | Dados brutos (batch + streaming) |
| Silver | `BUCKET_SILVER` | `335596040535--tech-challenge-fase-2-silver` | Dados tratados e quarentena |
| Gold | `BUCKET_GOLD` | `335596040535--tech-challenge-fase-2-gold` | Visões analíticas |

> No `.env`, informe **somente o nome do bucket** — sem `s3://` e sem pastas. O `config.py` monta os paths completos.

## Estrutura de paths

```
s3://{BUCKET_BRONZE}/bronze/batch/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_BRONZE}/bronze/streaming/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_SILVER}/silver/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_SILVER}/quarentena/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_GOLD}/gold/{visao}/ano={ano}/mes={mes}/dia={dia}/
```

**Entidades batch:** `uf`, `municipio`, `meta_brasil`, `meta_uf`, `meta_municipio`, `alunos`.

## Provisionamento

```bash
# Na raiz de pipeline-alfabetizacao/
bash infra/aws/setup-buckets.sh
```

O script:

1. Verifica existência dos 3 buckets
2. Cria arquivos `.keep` nos prefixos medalhão
3. Aplica tags FinOps (`project`, `environment`, `finops`, `layer`)

## Tags FinOps

| Tag | Valor |
|-----|-------|
| `project` | `tech-challenge-fase2` |
| `environment` | `dev` |
| `finops` | `tracked` |
| `layer` | `bronze` \| `silver` \| `gold` |

## Política de lifecycle (recomendação)

Para ambiente de desenvolvimento, versionamento permanece **desabilitado** (custo).

Em produção, considerar regra de lifecycle no bucket bronze:

- Objetos com mais de **90 dias** → transição para **S3 Glacier Instant Retrieval**
- Objetos em `quarentena/` com mais de **30 dias** → expiração automática

## IAM Role para Glue

| Campo | Valor |
|-------|-------|
| Nome | `glue-alfabetizacao-role` |
| ARN | `arn:aws:iam::335596040535:role/glue-alfabetizacao-role` |
| Trust principal | `glue.amazonaws.com` |
| Políticas | `iam-glue-policy.json` + `AWSGlueServiceRole` (managed) |

Criação via script:

```bash
bash infra/aws/setup-iam-role.sh
```

ARN esperado (substituir account-id):

```
arn:aws:iam::{ACCOUNT_ID}:role/glue-alfabetizacao-role
```

### Permissões (least privilege)

- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` nos 3 buckets medalhão
- `glue:*` no account (jobs, crawlers, catalog)
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

Não utilizar `AmazonS3FullAccess` em produção.

## Verificação

```bash
aws s3 ls s3://$BUCKET_BRONZE/bronze/
aws s3api get-bucket-tagging --bucket $BUCKET_BRONZE
aws iam get-role --role-name glue-alfabetizacao-role
```
