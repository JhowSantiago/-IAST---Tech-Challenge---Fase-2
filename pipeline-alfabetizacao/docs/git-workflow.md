# Versionamento com Git — Tech Challenge Fase 2

Este documento registra como o versionamento foi conduzido ao longo do desenvolvimento da pipeline, em atendimento ao requisito de histórico Git do Tech Challenge (commits claros, evolução por funcionalidade e integração responsável na branch principal).

## Organização do trabalho

O desenvolvimento acompanhou as etapas da solução (fundação, descoberta de dados, infraestrutura, batch, streaming, Silver, Gold, validação e documentação). Cada etapa gerou um conjunto de commits com escopo delimitado, de modo que o histórico reflita a construção progressiva do pipeline — e não um único commit final com toda a entrega.

Quando o trabalho foi feito em paralelo com branches, a convenção utilizada foi:

| Etapa | Branch | Conteúdo entregue |
|-------|--------|-------------------|
| Fundação | `feature/01-fundacao` | Estrutura do projeto, configuração e base de qualidade de dados |
| Descoberta | `feature/02-descoberta` | Exploração da Base dos Dados e dicionário |
| Infraestrutura | `feature/03-infra` | Buckets S3, IAM e catálogo Glue |
| Batch | `feature/04-batch` | Extração e carga Bronze em lote |
| Streaming | `feature/05-streaming` | Kafka e ingestão Bronze em streaming |
| Silver | `feature/06-silver` | Limpeza, validações e integração |
| Gold | `feature/07-gold` | Visões analíticas e consultas Athena |
| Validação | `feature/08-validacao` | Testes end-to-end e documentação FinOps |
| Entrega | `feature/09-entrega` | README, diagrama e documentação final |

Parte do histórico também foi consolidada diretamente em `main` com commits atômicos, mantendo a mesma disciplina de mensagens e escopo.

## Commits

As mensagens foram escritas em português, com um prefixo que indica o tipo de alteração (`feat`, `fix`, `docs`, `test`, `chore`) e um escopo curto. O objetivo é deixar evidente, no `git log`, o que mudou e por quê — por exemplo, se a alteração implementou uma camada, corrigiu um bug de qualidade de dados ou apenas atualizou documentação.

Arquivos com segredo (`.env`, `.env.viewer` e credenciais) ficaram fora do repositório; apenas templates de exemplo (`.env.example`, `.env.viewer.example`) foram versionados.

Exemplos retirados do histórico real do projeto:

```
feat(07-01): implementa camada Gold com visoes analiticas e scripts Athena
fix(07): deduplica alunos por id_aluno+ano para contagem Gold correta
test(validation): adiciona script de validacao end-to-end
docs(finops): documenta otimizacoes e estimativa de custos
feat(aws): publica jobs Glue na AWS e usuario viewer somente leitura
```

O repositório acumula mais de 25 commits nessa linha, o que permite acompanhar a evolução da Bronze até a Gold e o deploy na AWS.

## Pull Requests

Nas integrações via Pull Request, a descrição adotada segue três blocos: o que mudou, a motivação em relação aos requisitos do desafio, e como validar (scripts de teste e/ou consultas no Athena). Essa estrutura facilita a revisão — inclusive pelo avaliador — sem depender de contexto externo ao GitHub.

Exemplo do formato utilizado:

```markdown
## O que mudou
- Job Glue de carga Bronze para as entidades batch

## Por que
- Atender à ingestão histórica exigida no Tech Challenge

## Como testar
- Executar validar_pipeline.py
- No Athena: SELECT COUNT(*) FROM bronze_municipio
```

## Consulta do histórico

```bash
git log --oneline
```

## Acesso somente leitura para validação

Para que o avaliador consulte buckets, catálogo Glue e Athena sem permissão de escrita, foi provisionado o usuário IAM `alfabetizacao-viewer`. O script responsável é:

```powershell
python scripts/provisionar_usuario_viewer.py
```

As credenciais ficam apenas no arquivo local `.env.viewer` (não versionado), a partir do template `.env.viewer.example`.
