SELECT
    ano,
    CAST(id_municipio AS STRING) AS id_municipio,
    rede,
    taxa_alfabetizacao,
    meta_alfabetizacao_2024,
    meta_alfabetizacao_2025,
    meta_alfabetizacao_2026,
    meta_alfabetizacao_2027,
    meta_alfabetizacao_2028,
    meta_alfabetizacao_2029,
    meta_alfabetizacao_2030,
    nivel_alfabetizacao,
    percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`
WHERE ano IN (2023, 2024)
