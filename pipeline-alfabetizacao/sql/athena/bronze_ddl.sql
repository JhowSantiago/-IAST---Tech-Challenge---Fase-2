-- DDL Athena — Camada Bronze (templates)
-- Substituir {BUCKET_BRONZE} pelo nome do bucket antes de executar.
-- Executar MSCK REPAIR TABLE após cada ingestão batch.
-- Database: datalake_alfabetizacao

-- =============================================================================
-- UF (diretório territorial)
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_uf (
    id_uf STRING,
    sigla STRING,
    nome STRING,
    regiao STRING,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/uf/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Município (diretório territorial)
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_municipio (
    id_municipio STRING,
    sigla_uf STRING,
    nome STRING,
    nome_uf STRING,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/municipio/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Meta — Brasil
-- Observação: a coluna de avaliação foi nomeada ano_avaliacao para não conflitar
-- com a partição ano (data de ingestão). Para tabelas com ano de negócio, recomenda-se
-- o crawler bronze após a primeira carga.
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_meta_brasil (
    ano_avaliacao INT,
    rede STRING,
    taxa_alfabetizacao DOUBLE,
    meta_alfabetizacao_2024 DOUBLE,
    meta_alfabetizacao_2025 DOUBLE,
    meta_alfabetizacao_2026 DOUBLE,
    meta_alfabetizacao_2027 DOUBLE,
    meta_alfabetizacao_2028 DOUBLE,
    meta_alfabetizacao_2029 DOUBLE,
    meta_alfabetizacao_2030 DOUBLE,
    percentual_participacao DOUBLE,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/meta_brasil/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Meta — UF
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_meta_uf (
    ano_avaliacao INT,
    sigla_uf STRING,
    rede STRING,
    taxa_alfabetizacao DOUBLE,
    meta_alfabetizacao_2024 DOUBLE,
    meta_alfabetizacao_2025 DOUBLE,
    meta_alfabetizacao_2026 DOUBLE,
    meta_alfabetizacao_2027 DOUBLE,
    meta_alfabetizacao_2028 DOUBLE,
    meta_alfabetizacao_2029 DOUBLE,
    meta_alfabetizacao_2030 DOUBLE,
    percentual_participacao DOUBLE,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/meta_uf/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Meta — Município
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_meta_municipio (
    ano_avaliacao INT,
    id_municipio STRING,
    rede STRING,
    taxa_alfabetizacao DOUBLE,
    meta_alfabetizacao_2024 DOUBLE,
    meta_alfabetizacao_2025 DOUBLE,
    meta_alfabetizacao_2026 DOUBLE,
    meta_alfabetizacao_2027 DOUBLE,
    meta_alfabetizacao_2028 DOUBLE,
    meta_alfabetizacao_2029 DOUBLE,
    meta_alfabetizacao_2030 DOUBLE,
    nivel_alfabetizacao STRING,
    percentual_participacao DOUBLE,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/meta_municipio/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Alunos (microdados)
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.bronze_alunos (
    ano_avaliacao INT,
    id_municipio STRING,
    id_escola STRING,
    id_aluno STRING,
    caderno STRING,
    serie STRING,
    rede STRING,
    presenca STRING,
    preenchimento_caderno STRING,
    alfabetizado STRING,
    proficiencia DOUBLE,
    peso_aluno DOUBLE,
    _ingestion_timestamp TIMESTAMP,
    _ingestion_date STRING,
    _source_entity STRING,
    _job_name STRING,
    _record_hash STRING
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_BRONZE}/bronze/batch/alunos/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- =============================================================================
-- Reparo de partições (executar após cada ingestão)
-- =============================================================================
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_uf;
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_municipio;
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_meta_brasil;
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_meta_uf;
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_meta_municipio;
-- MSCK REPAIR TABLE datalake_alfabetizacao.bronze_alunos;
