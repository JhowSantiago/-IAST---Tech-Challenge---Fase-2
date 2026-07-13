# Pipeline Indicador Criança Alfabetizada

> A documentação principal do projeto está no **[README da raiz do repositório](../README.md)** — é o que o GitHub exibe na página inicial.

Este diretório contém o código-fonte, scripts, testes e documentação técnica detalhada da pipeline.

## Início rápido

```powershell
cd pipeline-alfabetizacao
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python tests/validation/validar_pipeline.py
```

## Documentação técnica

| Documento | Conteúdo |
|-----------|----------|
| [README principal](../README.md) | Documentação completa de entrega |
| [Arquitetura](docs/arquitetura.md) | Fluxo de dados e componentes AWS |
| [FinOps](docs/finops.md) | Otimizações e custos |
| [Deploy AWS](docs/deploy-aws.md) | Jobs Glue na nuvem |
| [Git workflow](docs/git-workflow.md) | Branches, commits e PRs |
