SELECT
    ano,
    CAST(id_municipio AS STRING) AS id_municipio,
    id_escola,
    id_aluno,
    caderno,
    serie,
    rede,
    presenca,
    preenchimento_caderno,
    alfabetizado,
    proficiencia,
    peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`
WHERE ano IN (2023, 2024)
