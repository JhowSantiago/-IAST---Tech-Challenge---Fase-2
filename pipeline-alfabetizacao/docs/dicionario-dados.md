# Dicionário de Dados

## 1. Objetivo

Este documento define as entidades de dados, suas fontes, chaves de relacionamento e convenções de tipagem adotadas na pipeline do Indicador Criança Alfabetizada. O mapeamento utiliza tabelas disponibilizadas pela plataforma [Base dos Dados](https://basedosdados.org/), no dataset `br_inep_avaliacao_alfabetizacao` e no diretório territorial `br_bd_diretorios_brasil`.

## 2. Entidades e fontes

| Entidade | ID BigQuery | Chave primária | Chaves estrangeiras | Granularidade |
|----------|-------------|----------------|---------------------|---------------|
| UF (diretório) | `br_bd_diretorios_brasil.uf` | `sigla_uf` | — | Unidade da Federação |
| Município (diretório) | `br_bd_diretorios_brasil.municipio` | `id_municipio` | `sigla_uf` | Município |
| Meta — Brasil | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` | `ano`, `rede` | — | Nacional por ano e rede |
| Meta — UF | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` | `ano`, `sigla_uf`, `rede` | — | UF por ano e rede |
| Meta — Município | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` | `ano`, `id_municipio`, `rede` | — | Município por ano e rede |
| Indicador — UF | `br_inep_avaliacao_alfabetizacao.uf` | `ano`, `sigla_uf`, `serie`, `rede` | — | Resultado de alfabetização por UF |
| Indicador — Município | `br_inep_avaliacao_alfabetizacao.municipio` | `ano`, `id_municipio`, `serie`, `rede` | — | Resultado de alfabetização por município |
| Alunos | `br_inep_avaliacao_alfabetizacao.alunos` | `id_aluno` | `id_municipio` | Aluno avaliado |

### 2.1 Atributos principais das tabelas de meta

As tabelas `meta_alfabetizacao_*` contêm as metas do Compromisso Nacional Criança Alfabetizada por ano de referência:

| Atributo | Tipo esperado | Descrição |
|----------|---------------|-----------|
| `taxa_alfabetizacao` | DOUBLE | Taxa observada no ano de referência |
| `meta_alfabetizacao_2024` … `meta_alfabetizacao_2030` | DOUBLE | Metas definidas para cada ano do compromisso |
| `percentual_participacao` | DOUBLE | Percentual de participação na avaliação |
| `nivel_alfabetizacao` | STRING | Presente apenas em `meta_alfabetizacao_municipio` |

### 2.2 Atributos principais das tabelas de indicador

As tabelas `uf` e `municipio` do dataset de alfabetização registram resultados da avaliação, não as metas:

| Atributo | Tipo esperado | Descrição |
|----------|---------------|-----------|
| `taxa_alfabetizacao` | DOUBLE | Percentual de alunos alfabetizados |
| `media_portugues` | DOUBLE | Média de proficiência em Língua Portuguesa |
| `proporcao_aluno_nivel_0` … `proporcao_aluno_nivel_8` | DOUBLE | Distribuição por nível de proficiência |

## 3. Relacionamentos entre entidades

| Entidade origem | Entidade destino | Chave de ligação |
|-----------------|------------------|------------------|
| Município (diretório) | Meta — Município | `id_municipio` |
| Município (diretório) | Indicador — Município | `id_municipio` |
| UF (diretório) | Meta — UF | `sigla_uf` |
| UF (diretório) | Indicador — UF | `sigla_uf` |
| Município (diretório) | Alunos | `id_municipio` |
| Meta — Município | Meta — UF | `sigla_uf` + `ano` + `rede` |

### 3.1 Regras de integração na camada Silver

- `meta_municipio` é integrada ao diretório territorial via `LEFT JOIN` em `id_municipio`;
- `meta_municipio` é enriquecida com metas estaduais via `LEFT JOIN` em `meta_uf` (`sigla_uf` + `ano` + `rede`);
- `indicador_municipio` é cruzado com `meta_municipio` para cálculo do gap entre resultado e meta;
- registros sem correspondência no diretório territorial são direcionados à área de **quarentena**, com motivo `municipio_inexistente`.

## 4. Convenções de tipagem

| Atributo | Tipo na camada Silver | Regra de validação |
|----------|----------------------|-------------------|
| `id_municipio` | STRING | Código IBGE de 7 dígitos, com zeros à esquerda preservados |
| `sigla_uf` | STRING | Duas letras maiúsculas |
| `ano` | INTEGER | Ano de referência ≥ 2023 |
| `taxa_alfabetizacao` | DOUBLE | Percentual entre 0 e 100 |
| `meta_alfabetizacao_*` | DOUBLE | Percentual entre 0 e 100 |
| `proficiencia` | DOUBLE | Escala Saeb; alfabetizado quando ≥ 743 pontos |

## 5. Atributos derivados

| Atributo | Definição | Camada |
|----------|-----------|--------|
| `gap_meta` | Diferença entre taxa observada e meta do ano (`taxa_alfabetizacao - meta_alfabetizacao_{ano}`) | Silver |
| `atingiu_meta` | Indicador booleano: `taxa_alfabetizacao >= meta_alfabetizacao_{ano}` | Silver |
| `delta_anual` | Variação da taxa de alfabetização em relação ao ano anterior | Gold |

## 6. Contrato de eventos (ingestão streaming)

Os eventos publicados no Apache Kafka seguem o contrato abaixo:

| Campo | Tipo | Obrigatoriedade | Descrição |
|-------|------|-----------------|-----------|
| `event_id` | UUID | Obrigatório | Identificador único do evento |
| `event_type` | STRING | Obrigatório | Tipo: `indicador_atualizado`, `meta_revisada` ou `medicao_nova` |
| `timestamp` | ISO 8601 | Obrigatório | Data e hora do evento |
| `payload.id_municipio` | STRING | Obrigatório | Código IBGE do município |
| `payload.sigla_uf` | STRING | Obrigatório | Sigla da unidade federativa |
| `payload.ano` | INTEGER | Obrigatório | Ano de referência |
| `payload.taxa_alfabetizacao` | DOUBLE | Condicional | Percentual de alfabetização |
| `payload.meta` | DOUBLE | Condicional | Meta de alfabetização do ano |

## 7. Metadados técnicos de auditoria

Para garantir rastreabilidade e reprocessamento, cada camada inclui metadados técnicos:

| Metadado | Bronze | Silver | Gold |
|----------|--------|--------|------|
| `_ingestion_timestamp` | Sim | Sim | — |
| `_ingestion_date` | Sim | Sim | — |
| `_source_entity` | Sim | — | — |
| `_job_name` | Sim | — | — |
| `_record_hash` | Sim | — | — |
| `_silver_processed_at` | — | Sim | — |
| `_gold_processed_at` | — | — | Sim |
| `_source_type` | Streaming | — | — |
