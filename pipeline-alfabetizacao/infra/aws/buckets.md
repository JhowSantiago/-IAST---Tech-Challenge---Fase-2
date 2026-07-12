# Armazenamento S3 — Arquitetura Medalhão

## 1. Introdução

Este documento descreve a organização do armazenamento em nuvem adotado na pipeline do Indicador Criança Alfabetizada (ICA). A estrutura segue o padrão **Medalhão**, com separação física em três buckets Amazon S3, conforme orientação da disciplina de ETL Pipelines (Aula 3.3).

A separação por camada permite aplicar políticas distintas de acesso, custo e retenção, além de facilitar a auditoria dos dados em cada estágio do processamento.

## 2. Buckets e variáveis de ambiente

| Camada | Variável | Finalidade |
|--------|----------|------------|
| Bronze | `BUCKET_BRONZE` | Persistência dos dados brutos (batch e streaming) |
| Silver | `BUCKET_SILVER` | Dados tratados, validados e área de quarentena |
| Gold | `BUCKET_GOLD` | Visões analíticas para consumo |

As variáveis devem conter **apenas o nome do bucket**, sem prefixo `s3://` e sem barras de diretório. Os caminhos completos são montados programaticamente em `src/common/config.py`.

**Exemplo de configuração (conta de desenvolvimento):**

| Camada | Bucket |
|--------|--------|
| Bronze | `335596040535--tech-challenge-fase-2-bronze` |
| Silver | `335596040535--tech-challenge-fase-2-silver` |
| Gold | `335596040535--tech-challenge-fase-2-gold` |

Região utilizada: `us-east-2` (Ohio).

## 3. Estrutura de diretórios

Os objetos são organizados com particionamento por data de ingestão (`ano`, `mes`, `dia`):

```
s3://{BUCKET_BRONZE}/bronze/batch/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_BRONZE}/bronze/streaming/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_SILVER}/silver/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_SILVER}/quarentena/{entidade}/ano={ano}/mes={mes}/dia={dia}/
s3://{BUCKET_GOLD}/gold/{visao}/ano={ano}/mes={mes}/dia={dia}/
```

As seis entidades da ingestão batch são: `uf`, `municipio`, `meta_brasil`, `meta_uf`, `meta_municipio` e `alunos`.

## 4. Provisionamento da estrutura

Após a criação manual dos buckets no console AWS, a estrutura interna pode ser inicializada com:

```bash
bash infra/aws/setup-buckets.sh
```

Em ambientes Windows, utilizar o script equivalente:

```bash
python scripts/provisionar_buckets.py
```

O procedimento verifica a existência dos buckets, cria marcadores nos prefixos medalhão e aplica as tags de controle de custo.

## 5. Tags de FinOps

Cada bucket recebe as tags abaixo para rastreabilidade de custos no ambiente acadêmico:

| Tag | Valor |
|-----|-------|
| `project` | `tech-challenge-fase2` |
| `environment` | `dev` |
| `finops` | `tracked` |
| `layer` | `bronze`, `silver` ou `gold` |

## 6. Política de ciclo de vida

No ambiente de desenvolvimento, o versionamento dos buckets permanece desabilitado para redução de custos.

Para um cenário produtivo, recomenda-se avaliar regras de transição dos objetos da camada bronze com mais de 90 dias para o S3 Glacier Instant Retrieval, bem como expiração automática dos registros em quarentena após 30 dias.

## 7. Papel IAM para o AWS Glue

O serviço Glue requer um papel IAM dedicado para leitura e escrita nos buckets medalhão.

| Atributo | Valor |
|----------|-------|
| Nome | `glue-alfabetizacao-role` |
| ARN | `arn:aws:iam::335596040535:role/glue-alfabetizacao-role` |
| Principal de confiança | `glue.amazonaws.com` |
| Política inline | `iam-glue-role.json` |
| Política gerenciada | `AWSGlueServiceRole` |

A criação do papel pode ser realizada com:

```bash
bash infra/aws/setup-iam-role.sh
```

O script aplica o princípio de menor privilégio, concedendo acesso somente aos três buckets do projeto, ao Glue Data Catalog e aos grupos de log do CloudWatch. Não se recomenda o uso de `AmazonS3FullAccess` em ambientes produtivos.

## 8. Verificação

```bash
aws s3 ls s3://$BUCKET_BRONZE/bronze/
aws s3api get-bucket-tagging --bucket $BUCKET_BRONZE
aws iam get-role --role-name glue-alfabetizacao-role
```
