# Glue Data Catalog e Amazon Athena

## 1. Introdução

O AWS Glue Data Catalog funciona como metastore da solução, permitindo consultas SQL via Amazon Athena sobre os arquivos Parquet armazenados no S3. Este componente integra a camada de armazenamento medalhão à camada de consumo analítico, conforme a arquitetura definida no Tech Challenge.

## 2. Database

| Atributo | Valor |
|----------|-------|
| Nome | `datalake_alfabetizacao` |
| Região | `us-east-2` |

O database agrupa as tabelas externas das camadas bronze, silver e gold.

## 3. Crawlers da camada bronze

Na camada bronze, adota-se a inferência automática de schema por meio de crawlers Glue, **um crawler por entidade**, apontando para a subpasta correspondente no S3. Isso evita a criação de uma tabela única heterogênea na raiz `bronze/batch/`.

| Crawler | Caminho monitorado | Tabela inferida |
|---------|-------------------|-----------------|
| `crawler-bronze-uf` | `s3://{BUCKET_BRONZE}/bronze/batch/uf/` | `uf` |
| `crawler-bronze-municipio` | `s3://{BUCKET_BRONZE}/bronze/batch/municipio/` | `municipio` |
| `crawler-bronze-meta_brasil` | `s3://{BUCKET_BRONZE}/bronze/batch/meta_brasil/` | `meta_brasil` |
| `crawler-bronze-meta_uf` | `s3://{BUCKET_BRONZE}/bronze/batch/meta_uf/` | `meta_uf` |
| `crawler-bronze-meta_municipio` | `s3://{BUCKET_BRONZE}/bronze/batch/meta_municipio/` | `meta_municipio` |
| `crawler-bronze-alunos` | `s3://{BUCKET_BRONZE}/bronze/batch/alunos/` | `alunos` |
| `crawler-bronze-streaming` | `s3://{BUCKET_BRONZE}/bronze/streaming/` | (eventos) |

Registro dos crawlers:

```bash
bash scripts/registrar_crawlers.sh
```

Alternativa para ambientes sem shell bash:

```bash
python scripts/provisionar_iam_glue.py
```

Executar todos os crawlers batch após uma carga:

```bash
python scripts/executar_crawlers_bronze.py
```

Para executar um crawler manualmente:

```bash
aws glue start-crawler --name crawler-bronze-alunos --region us-east-2
```

### Particionamento (importante)

Os dados no bronze estão **completos** — `mes` e `dia` indicam a **data da ingestão** (ex.: `07` e `12` = carga de 12/07/2026), não um recorte do conteúdo.

| Entidade | Partição `ano` | Partição `mes` / `dia` | Conteúdo do arquivo |
|----------|----------------|------------------------|---------------------|
| `uf`, `municipio` | Ano da ingestão (ex. `2026`) | Data da ingestão | **Todos** os registros territoriais |
| `meta_*`, `alunos` | Ano de avaliação INEP (`2023`, `2024`) | Data da ingestão | **Todos** os registros daquele ano |

Exemplo: `alunos/ano=2023/mes=07/dia=12/` contém ~1,7M alunos de 2023; `ano=2024/...` contém ~2,1M de 2024. O total (~3,87M) corresponde à extração completa filtrada por `WHERE ano IN (2023, 2024)`.

## 4. Estratégia de catalogação por camada

| Camada | Método | Justificativa |
|--------|--------|---------------|
| Bronze | Crawler Glue | Dados brutos com schema próximo ao da origem; inferência reduz esforço de manutenção |
| Silver | DDL manual | Schema controlado após transformação e validação de qualidade |
| Gold | DDL manual | Visões analíticas com atributos derivados (`gap_meta`, `atingiu_meta`) |

As definições de schema das camadas silver e gold estão em `src/dq/schemas.py`. Os scripts DDL correspondentes serão adicionados em `sql/athena/` conforme a evolução da pipeline.

A inferência automática não é adequada para as camadas silver e gold porque renomeações (por exemplo, `sigla` → `sigla_uf`), colunas calculadas e tipos explícitos precisam ser preservados para garantir consistência nas consultas.

## 5. DDL da camada bronze

Os templates `CREATE EXTERNAL TABLE` estão em `sql/athena/bronze_ddl.sql`, cobrindo as seis entidades da ingestão batch.

Procedimento de uso:

1. Substituir o placeholder `{BUCKET_BRONZE}` pelo nome do bucket bronze.
2. Executar cada `CREATE EXTERNAL TABLE` no Athena.
3. Após a ingestão, executar `MSCK REPAIR TABLE` para registrar novas partições.

## 6. Workgroup Athena (recomendação)

Recomenda-se a criação de um workgroup dedicado (`ica-analytics`) com destino dos resultados em `s3://{BUCKET_SILVER}/athena-results/`, a fim de controlar custos e centralizar os arquivos temporários de consulta.

## 7. Verificação

```bash
aws glue get-database --name datalake_alfabetizacao --region us-east-2
aws glue list-crawlers --region us-east-2
```

Exemplo de consulta após ingestão e reparo de partições:

```sql
SELECT COUNT(*) FROM datalake_alfabetizacao.alunos;
SELECT COUNT(*) FROM datalake_alfabetizacao.meta_municipio;
```
