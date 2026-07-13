# Tech Challenge — Fase 2: Pipeline Indicador Criança Alfabetizada

Repositório da entrega do **Tech Challenge Fase 2** (Pós Tech em AI Scientist — FIAP/POSTECH).

## Solução

Pipeline de dados **híbrida** (batch + streaming) na AWS para integrar fontes educacionais do **Indicador Criança Alfabetizada (ICA)**, com arquitetura Medalhão (Bronze → Silver → Gold) e consumo via Amazon Athena.

## Documentação principal

Toda a documentação de execução, arquitetura, validação e entrega está em:

**[pipeline-alfabetizacao/README.md](pipeline-alfabetizacao/README.md)**

## Início rápido

```powershell
cd pipeline-alfabetizacao
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python tests/validation/validar_pipeline.py
```

## Estrutura

```
├── pipeline-alfabetizacao/   # Código, scripts, docs e testes da pipeline
└── [IAST] - Tech Challenge - Fase 2.pdf   # Enunciado do desafio
```
