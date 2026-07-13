-- DDL Athena — Camada Silver
-- Substituir {BUCKET_SILVER} pelo bucket silver antes de executar.
-- Executar MSCK REPAIR TABLE após cada carga Silver.
-- Database: datalake_alfabetizacao

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_uf (
    id_uf STRING,
    sigla_uf STRING,
    nome_uf STRING,
    regiao STRING,
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/uf/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_municipio (
    id_municipio STRING,
    sigla_uf STRING,
    nome_municipio STRING,
    nome_uf STRING,
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (ano STRING, mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/municipio/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_meta_brasil (
    ano INT,
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
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/meta_brasil/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_meta_uf (
    ano INT,
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
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/meta_uf/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_meta_municipio (
    ano INT,
    id_municipio STRING,
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
    nivel_alfabetizacao STRING,
    percentual_participacao DOUBLE,
    gap_meta DOUBLE,
    atingiu_meta BOOLEAN,
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/meta_municipio/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_alunos (
    ano INT,
    id_municipio STRING,
    id_escola STRING,
    id_aluno STRING,
    serie STRING,
    rede STRING,
    alfabetizado STRING,
    proficiencia DOUBLE,
    peso_aluno DOUBLE,
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/alunos/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.silver_municipio_indicador_completo (
    ano INT,
    id_municipio STRING,
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
    nivel_alfabetizacao STRING,
    percentual_participacao DOUBLE,
    gap_meta DOUBLE,
    atingiu_meta BOOLEAN,
    nome_municipio STRING,
    nome_uf STRING,
    indicador_uf DOUBLE,
    event_id STRING,
    _source_type STRING,
    _silver_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_SILVER}/silver/municipio_indicador_completo/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_uf;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_municipio;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_meta_brasil;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_meta_uf;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_meta_municipio;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_alunos;
-- MSCK REPAIR TABLE datalake_alfabetizacao.silver_municipio_indicador_completo;
