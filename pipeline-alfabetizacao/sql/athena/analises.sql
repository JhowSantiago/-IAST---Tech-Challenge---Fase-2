-- Queries analíticas — Camada Gold
-- Database: datalake_alfabetizacao

-- Top 10 municípios com maior gap negativo (abaixo da meta)
SELECT nome, sigla_uf, ano, pct_alfabetizados, meta_pct, gap_meta
FROM datalake_alfabetizacao.gold_indicador_municipio
WHERE atingiu_meta = false
ORDER BY gap_meta ASC
LIMIT 10;

-- Evolução nacional do indicador (média por ano)
SELECT ano, ROUND(AVG(pct_alfabetizados), 2) AS media_nacional
FROM datalake_alfabetizacao.gold_evolucao_temporal
GROUP BY ano
ORDER BY ano;

-- UFs que mais melhoraram (delta entre anos consecutivos, último ano disponível)
SELECT sigla_uf, ano, delta_percentual, taxa_media
FROM datalake_alfabetizacao.gold_meta_vs_resultado
WHERE delta_percentual IS NOT NULL
ORDER BY delta_percentual DESC
LIMIT 10;

-- Ranking de UFs por taxa média no ano mais recente
SELECT sigla_uf, regiao, taxa_media, meta_media, gap_medio, ranking_taxa
FROM datalake_alfabetizacao.gold_meta_vs_resultado
WHERE ano = (SELECT MAX(ano) FROM datalake_alfabetizacao.gold_meta_vs_resultado)
ORDER BY ranking_taxa;

-- Municípios que atingiram meta vs abaixo (agregado nacional)
SELECT
    SUM(municipios_acima_meta) AS total_acima_meta,
    SUM(municipios_abaixo_meta) AS total_abaixo_meta
FROM datalake_alfabetizacao.gold_meta_vs_resultado
WHERE ano = (SELECT MAX(ano) FROM datalake_alfabetizacao.gold_meta_vs_resultado);
