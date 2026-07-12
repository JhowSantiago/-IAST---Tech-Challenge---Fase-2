# Pipeline Híbrida — Indicador Criança Alfabetizada

**Tech Challenge — Fase 2 | Pós Tech em AI Scientist (FIAP/POSTECH)**

## Apresentação

Este repositório contém a implementação de uma pipeline de dados híbrida (batch + streaming) desenvolvida para integrar fontes educacionais relacionadas ao **Indicador Criança Alfabetizada (ICA)**. A solução utiliza a plataforma [Base dos Dados](https://basedosdados.org/) como fonte primária e segue a arquitetura Medalhão (Bronze → Silver → Gold), implantada na Amazon Web Services (AWS).

O projeto atende ao desafio proposto na disciplina de preparação de dados, simulando o trabalho de uma equipe de engenharia de dados em uma organização pública de análise educacional.

## Contexto do problema

A alfabetização na infância é um dos pilares do desenvolvimento educacional brasileiro. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados e municípios para garantir que todas as crianças estejam alfabetizadas ao final do 2º ano do ensino fundamental até 2030.

Com base na Pesquisa Alfabetiza Brasil (2023), o Inep definiu o ponto de corte de **743 pontos** na escala de proficiência do Saeb. O ICA expressa o percentual de estudantes que atingem esse patamar, subsidiando políticas públicas baseadas em evidências.

## Estrutura do repositório

```
pipeline-alfabetizacao/
├── src/
│   ├── common/       # Configuração e logging compartilhados
│   ├── batch/        # Extração da Base dos Dados
│   ├── streaming/    # Produtor e consumidor Kafka
│   ├── bronze/       # Jobs de ingestão (AWS Glue)
│   ├── silver/       # Transformação e integração de dados
│   ├── gold/         # Agregações analíticas
│   └── dq/           # Regras de qualidade de dados
├── sql/athena/       # Definições DDL e consultas analíticas
├── infra/aws/        # Infraestrutura AWS (S3, IAM, Glue Catalog)
├── docker/           # Ambiente Apache Kafka local
├── docs/             # Documentação técnica da solução
├── scripts/          # Scripts de configuração e utilitários
└── tests/validation/ # Validação end-to-end da pipeline
```

## Documentação técnica

| Documento | Conteúdo |
|-----------|----------|
| [Arquitetura da solução](docs/arquitetura.md) | Diagrama, fluxo de dados e componentes AWS |
| [Dicionário de dados](docs/dicionario-dados.md) | Entidades, chaves e convenções de tipagem |
| [Decisões arquiteturais](docs/decisoes-arquiteturais.md) | Trade-offs e justificativas técnicas |

## Pré-requisitos

- Python 3.10 ou superior
- Conta na Amazon Web Services (S3, Glue, Athena)
- Projeto no Google Cloud Platform com BigQuery habilitado (Base dos Dados)
- Docker Desktop (para execução do Apache Kafka local)

## Configuração do ambiente

```bash
cd pipeline-alfabetizacao
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
copy .env.example .env        # Preencher credenciais
```

### Variáveis de ambiente

| Variável | Descrição |
|----------|-----------|
| `BILLING_PROJECT_ID` | ID do projeto GCP para consultas BigQuery |
| `AWS_ACCESS_KEY_ID` | Chave de acesso AWS |
| `AWS_SECRET_ACCESS_KEY` | Chave secreta AWS |
| `AWS_DEFAULT_REGION` | Região AWS (ex.: `us-east-1`) |
| `BUCKET_SOR` | Bucket S3 — camada Bronze |
| `BUCKET_SOT` | Bucket S3 — camada Silver |
| `BUCKET_SPEC` | Bucket S3 — camada Gold |
| `KAFKA_BOOTSTRAP_SERVERS` | Endereço do broker Kafka |

## Referências

- [Tech Challenge — Fase 2](../[IAST]%20-%20Tech%20Challenge%20-%20Fase%202.pdf)
- [Base dos Dados](https://basedosdados.org/)
- [Compromisso Nacional Criança Alfabetizada](https://www.gov.br/mec/pt-br/crianca-alfabetizada)
