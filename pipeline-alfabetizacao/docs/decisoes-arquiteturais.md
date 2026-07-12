# Decisões Arquiteturais

## 1. Introdução

Este documento registra as principais decisões técnicas adotadas no desenvolvimento da pipeline de dados do Tech Challenge — Fase 2. Para cada decisão, são apresentadas as alternativas avaliadas, os trade-offs considerados e a justificativa da escolha final.

## 2. Provedor de nuvem

### Alternativas avaliadas

| Alternativa | Vantagens | Desvantagens | Decisão |
|-------------|-----------|--------------|---------|
| Amazon Web Services (AWS) | Ecossistema maduro para Data Lakes; serviços serverless (Glue, Athena); ampla documentação | Custos em dólar | **Adotada** |
| Google Cloud Platform (GCP) | Integração nativa com Base dos Dados (BigQuery) | Arquitetura de pipeline distinta da adotada no projeto | Utilizada apenas como origem batch |
| Microsoft Azure | — | Menor aderência ao escopo técnico do desafio | Descartada |

### Justificativa

A AWS foi selecionada como provedor principal por oferecer serviços integrados para construção de pipelines de dados em larga escala: Amazon S3 para armazenamento, AWS Glue para processamento e Amazon Athena para consultas analíticas. A origem batch permanece no BigQuery (Base dos Dados), enquanto todo o processamento e armazenamento analítico ocorre na AWS.

## 3. Modelo de ingestão: batch versus streaming

| Critério | Ingestão batch | Ingestão streaming |
|----------|----------------|-------------------|
| Finalidade | Carga de dados históricos e estruturados | Simulação de atualizações em tempo quase real |
| Fonte | Base dos Dados (BigQuery) | Apache Kafka |
| Latência | Agendamento periódico (ex.: diário) | Segundos |
| Volume | Alto (tabelas completas) | Baixo (eventos incrementais) |

**Decisão:** ambos os modelos coexistem na camada Bronze, em paths distintos (`batch/` e `streaming/`). A camada Silver realiza a integração e deduplicação dos registros provenientes das duas fontes.

## 4. Armazenamento e consulta analítica

| Alternativa | Papel | Decisão |
|-------------|-------|---------|
| Data Lake (Amazon S3) | Armazenamento de todas as camadas | **Adotado** |
| Amazon Athena | Motor de consultas SQL serverless | **Adotado** |
| Amazon Redshift | Data Warehouse dedicado | Descartado (custo elevado para escopo acadêmico) |

A arquitetura adotada corresponde a um **lakehouse leve**: dados persistidos em S3 no formato Parquet, com consultas realizadas via Athena sem necessidade de infraestrutura dedicada de warehouse.

## 5. Organização dos jobs de processamento

| Alternativa | Vantagens | Desvantagens | Decisão |
|-------------|-----------|--------------|---------|
| Jobs separados por camada (Bronze, Silver, Gold) | Facilita depuração, reprocessamento incremental e manutenção | Maior número de jobs a gerenciar | **Adotada** |
| Job monolítico único | Deploy simplificado | Falha em uma etapa compromete todo o pipeline | Descartada |

## 6. Infraestrutura de streaming

| Alternativa | Vantagens | Desvantagens | Decisão |
|-------------|-----------|--------------|---------|
| Apache Kafka via Docker (ambiente local) | Sem custo adicional; adequado para demonstração | Não representa ambiente produtivo | **Adotada** |
| Amazon MSK (Kafka gerenciado) | Alta disponibilidade em produção | Custo estimado superior a US$ 150/mês | Descartada |

## 7. Qualidade de dados

A pipeline implementa validações automatizadas em cada camada, verificando:

- **Completude** — presença mínima esperada de valores;
- **Validade** — conformidade com domínios e formatos definidos;
- **Consistência** — coerência entre atributos relacionados;
- **Unicidade** — ausência de duplicatas indevidas.

Registros que não atendem aos critérios de qualidade são direcionados à área de **quarentena** (`s3://{BUCKET_SILVER}/quarentena/`), sem interrupção do fluxo principal de processamento. Falhas em verificações críticas interrompem o job correspondente.

## 8. Otimização de custos (FinOps)

As seguintes práticas foram adotadas para controle de custos operacionais:

| Prática | Benefício esperado |
|---------|-------------------|
| Formato Parquet com compressão SNAPPY | Redução de até 80% no volume de armazenamento e scan em relação ao CSV |
| Particionamento por `ano/mes/dia` | Evita varredura completa (full scan) nas consultas Athena |
| Workers Glue G.1X (mínimo necessário) | Redução de aproximadamente 50% no custo de processamento |
| Consultas com `SELECT` explícito no BigQuery | Redução de bytes processados na origem |
| DDL manual nas camadas Silver e Gold | Evita re-inferência desnecessária de schemas |

## 9. Dependências de infraestrutura

| Serviço | Finalidade | Momento de configuração |
|---------|------------|------------------------|
| Conta AWS com IAM e access keys | S3, Glue, Athena | Antes da implantação da infraestrutura |
| Projeto Google Cloud (BigQuery) | Consulta à Base dos Dados | Antes da extração batch |
| Docker Desktop | Execução do Apache Kafka local | Antes da ingestão streaming |
