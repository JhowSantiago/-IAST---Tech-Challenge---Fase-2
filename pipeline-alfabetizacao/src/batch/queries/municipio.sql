SELECT
    CAST(id_municipio AS STRING) AS id_municipio,
    sigla_uf,
    nome,
    nome_uf
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
