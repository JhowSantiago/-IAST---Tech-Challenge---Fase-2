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

Na camada bronze, adota-se a inferência automática de schema por meio de crawlers Glue, prática alinhada ao material da Aula 3.3.

| Crawler | Caminho monitorado | Execução |
|---------|-------------------|----------|
| `crawler-bronze-batch` | `s3://{BUCKET_BRONZE}/bronze/batch/` | Após cada carga batch |
| `crawler-bronze-streaming` | `s3://{BUCKET_BRONZE}/bronze/streaming/` | Após ingestão de eventos |

Registro dos crawlers:

```bash
bash scripts/registrar_crawlers.sh
```

Alternativa para ambientes sem shell bash:

```bash
python scripts/provisionar_iam_glue.py
```

Para executar um crawler manualmente:

```bash
aws glue start-crawler --name crawler-bronze-batch --region us-east-2
```

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
SELECT COUNT(*) FROM datalake_alfabetizacao.bronze_meta_municipio;
```
