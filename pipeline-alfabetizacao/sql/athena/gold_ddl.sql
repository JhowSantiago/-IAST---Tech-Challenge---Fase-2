-- DDL Athena — Camada Gold (templates)
-- Substituir {BUCKET_GOLD} pelo bucket gold antes de executar.
-- Database: datalake_alfabetizacao

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.gold_indicador_municipio (
    id_municipio STRING,
    nome STRING,
    sigla_uf STRING,
    nome_uf STRING,
    ano INT,
    rede STRING,
    pct_alfabetizados DOUBLE,
    meta_pct DOUBLE,
    gap_meta DOUBLE,
    atingiu_meta BOOLEAN,
    indicador_uf DOUBLE,
    nivel_alfabetizacao STRING,
    percentual_participacao DOUBLE,
    total_alunos_avaliados BIGINT,
    _gold_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_GOLD}/gold/indicador_municipio/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.gold_meta_vs_resultado (
    sigla_uf STRING,
    ano INT,
    taxa_media DOUBLE,
    meta_media DOUBLE,
    gap_medio DOUBLE,
    municipios_total BIGINT,
    municipios_acima_meta BIGINT,
    municipios_abaixo_meta BIGINT,
    ranking_taxa INT,
    taxa_anterior DOUBLE,
    delta_percentual DOUBLE,
    regiao STRING,
    nome_uf STRING,
    _gold_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_GOLD}/gold/meta_vs_resultado/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS datalake_alfabetizacao.gold_evolucao_temporal (
    id_municipio STRING,
    nome STRING,
    sigla_uf STRING,
    nome_uf STRING,
    rede STRING,
    ano INT,
    pct_alfabetizados DOUBLE,
    pct_anterior DOUBLE,
    delta_percentual DOUBLE,
    delta_anual DOUBLE,
    meta_pct DOUBLE,
    gap_meta DOUBLE,
    atingiu_meta BOOLEAN,
    _gold_processed_at TIMESTAMP
)
PARTITIONED BY (mes STRING, dia STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET_GOLD}/gold/evolucao_temporal/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- MSCK REPAIR TABLE datalake_alfabetizacao.gold_indicador_municipio;
-- MSCK REPAIR TABLE datalake_alfabetizacao.gold_meta_vs_resultado;
-- MSCK REPAIR TABLE datalake_alfabetizacao.gold_evolucao_temporal;
