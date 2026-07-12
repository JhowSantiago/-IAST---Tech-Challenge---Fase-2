# Dicionário de Dados

## 1. Objetivo

Este documento define as entidades de dados, suas fontes, chaves de relacionamento e convenções de tipagem adotadas na pipeline do Indicador Criança Alfabetizada. O mapeamento foi **validado em 2026-07-12** contra dados reais do BigQuery (ver `02-DISCOVERY.md`).

## 2. Entidades e fontes

| Entidade batch | ID BigQuery | Linhas | Anos | Chave primária |
|----------------|-------------|--------|------|----------------|
| UF (diretório) | `br_bd_diretorios_brasil.uf` | 27 | — | `sigla` → padronizada como `sigla_uf` |
| Município (diretório) | `br_bd_diretorios_brasil.municipio` | 5.571 | — | `id_municipio` |
| Meta — Brasil | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` | 3 | 2023–2025 | `ano`, `rede` |
| Meta — UF | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` | 81 | 2023–2025 | `ano`, `sigla_uf`, `rede` |
| Meta — Município | `br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` | 10.704 | 2023–2024 | `ano`, `id_municipio`, `rede` |
| Alunos | `br_inep_avaliacao_alfabetizacao.alunos` | 3.867.999 | 2023–2024 | `id_aluno` |

### 2.1 Entidades complementares (integração Silver)

| Entidade | ID BigQuery | Finalidade |
|----------|-------------|------------|
| Indicador — UF | `br_inep_avaliacao_alfabetizacao.uf` | Taxa e distribuição por nível na UF |
| Indicador — Município | `br_inep_avaliacao_alfabetizacao.municipio` | Taxa e distribuição por nível no município |

## 3. Schemas validados

### 3.1 UF (diretório)

| Coluna origem | Coluna Silver | Tipo | Observação |
|---------------|---------------|------|------------|
| `id_uf` | `id_uf` | STRING | Código IBGE da UF |
| `sigla` | `sigla_uf` | STRING | Renomeada na Silver |
| `nome` | `nome_uf` | STRING | Nome da unidade federativa |
| `regiao` | `regiao` | STRING | Região geográfica |

### 3.2 Município (diretório)

| Coluna | Tipo | Papel |
|--------|------|-------|
| `id_municipio` | STRING (7 dígitos) | Chave primária |
| `sigla_uf` | STRING (2 chars) | Chave estrangeira |
| `nome` | STRING | Nome do município |
| `nome_uf` | STRING | Nome da UF |

Demais 23 colunas territoriais são opcionais na Bronze e descartadas na Silver.

### 3.3 Tabelas de meta (`meta_alfabetizacao_*`)

| Coluna | Tipo | Papel |
|--------|------|-------|
| `ano` | INTEGER | Chave |
| `sigla_uf` / `id_municipio` | STRING | Chave (conforme granularidade) |
| `rede` | STRING | Dimensão (`Pública`) |
| `taxa_alfabetizacao` | FLOAT | Resultado observado no ano |
| `meta_alfabetizacao_2024` | FLOAT | Meta para 2024 |
| `meta_alfabetizacao_2025` | FLOAT | Meta para 2025 |
| `meta_alfabetizacao_2026` | FLOAT | Meta para 2026 |
| `meta_alfabetizacao_2027` | FLOAT | Meta para 2027 |
| `meta_alfabetizacao_2028` | FLOAT | Meta para 2028 |
| `meta_alfabetizacao_2029` | FLOAT | Meta para 2029 |
| `meta_alfabetizacao_2030` | FLOAT | Meta para 2030 |
| `percentual_participacao` | FLOAT | Participação na avaliação |
| `nivel_alfabetizacao` | STRING | Apenas em `meta_alfabetizacao_municipio` |

### 3.4 Tabelas de indicador (`uf`, `municipio` do INEP)

| Coluna | Tipo | Papel |
|--------|------|-------|
| `ano` | INTEGER | Chave |
| `sigla_uf` / `id_municipio` | STRING | Chave |
| `serie` | STRING | Série avaliada |
| `rede` | STRING | Rede de ensino |
| `taxa_alfabetizacao` | FLOAT | Percentual de alfabetizados |
| `media_portugues` | FLOAT | Média de proficiência |
| `proporcao_aluno_nivel_0` … `8` | FLOAT | Distribuição por nível Saeb |

### 3.5 Alunos

| Coluna | Tipo | Papel |
|--------|------|-------|
| `ano` | INTEGER | Chave |
| `id_municipio` | STRING | Chave estrangeira |
| `id_escola` | STRING | Identificador da escola |
| `id_aluno` | STRING | Chave primária |
| `caderno` | STRING | Caderno aplicado |
| `serie` | STRING | Série do aluno |
| `rede` | STRING | Rede de ensino |
| `presenca` | STRING | Presença na avaliação |
| `preenchimento_caderno` | STRING | Preenchimento do caderno |
| `alfabetizado` | STRING | `Sim` ou `Não` (corte 743 pts) |
| `proficiencia` | FLOAT | Pontuação Saeb |
| `peso_aluno` | FLOAT | Peso amostral |

## 4. Relacionamentos

| Origem | Destino | Chave |
|--------|---------|-------|
| Município (dir.) | Meta — Município | `id_municipio` |
| Município (dir.) | Indicador — Município | `id_municipio` |
| Município (dir.) | Alunos | `id_municipio` |
| UF (dir.) | Meta — UF | `sigla_uf` |
| UF (dir.) | Indicador — UF | `sigla_uf` |
| Meta — Município | Meta — UF | `sigla_uf` + `ano` + `rede` |

### 4.1 Regras de integração na Silver

- `meta_municipio` integrada ao diretório via `LEFT JOIN` em `id_municipio`;
- `meta_municipio` enriquecida com `meta_uf` via `sigla_uf`, `ano` e `rede`;
- `indicador_municipio` cruzado com `meta_municipio` para `gap_meta` e `atingiu_meta`;
- registros sem município no diretório → quarentena (`municipio_inexistente`).

## 5. Convenções de tipagem

| Atributo | Tipo Silver | Validação |
|----------|-------------|-----------|
| `id_municipio` | STRING | Regex `^\d{7}$` |
| `sigla_uf` | STRING | Regex `^[A-Z]{2}$` |
| `ano` | INTEGER | ≥ 2023 |
| `taxa_alfabetizacao` | DOUBLE | Intervalo [0, 100] |
| `meta_alfabetizacao_*` | DOUBLE | Intervalo [0, 100] |
| `proficiencia` | DOUBLE | Alfabetizado se ≥ 743 |

## 6. Atributos derivados

| Atributo | Definição | Camada |
|----------|-----------|--------|
| `gap_meta` | `taxa_alfabetizacao - meta_alfabetizacao_{ano}` | Silver |
| `atingiu_meta` | `taxa_alfabetizacao >= meta_alfabetizacao_{ano}` | Silver |
| `delta_anual` | Variação da taxa em relação ao ano anterior | Gold |

## 7. Contrato de eventos (streaming)

| Campo | Tipo | Obrigatório | Descrição |
|-------|------|-------------|-----------|
| `event_id` | UUID | Sim | Identificador único |
| `event_type` | STRING | Sim | `indicador_atualizado`, `meta_revisada`, `medicao_nova` |
| `timestamp` | ISO 8601 | Sim | Data/hora do evento |
| `payload.id_municipio` | STRING | Sim | Código IBGE |
| `payload.sigla_uf` | STRING | Sim | Sigla da UF |
| `payload.ano` | INTEGER | Sim | Ano de referência |
| `payload.taxa_alfabetizacao` | DOUBLE | Condicional | Taxa observada |
| `payload.meta` | DOUBLE | Condicional | Meta do ano |

## 8. Metadados técnicos

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

## 9. Entidades batch

```python
ENTIDADES_BATCH = [
    "uf",
    "municipio",
    "meta_uf",
    "meta_municipio",
    "meta_brasil",
    "alunos",
]
```
