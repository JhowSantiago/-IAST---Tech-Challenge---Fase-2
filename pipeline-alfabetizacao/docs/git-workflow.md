# Workflow Git — Tech Challenge Fase 2

Este documento descreve o padrão de versionamento adotado no projeto, conforme requisito RF-09.

## Estratégia de branches

Cada fase do roadmap foi desenvolvida em commits atômicos na branch `main`. Para equipes que preferem branches por feature:

| Fase | Branch sugerida | Escopo |
|------|-----------------|--------|
| 01 | `feature/01-fundacao` | Estrutura, config, DQ base |
| 02 | `feature/02-descoberta` | Exploração Base dos Dados |
| 03 | `feature/03-infra` | S3, IAM, Glue Catalog |
| 04 | `feature/04-batch` | Extração + Bronze batch |
| 05 | `feature/05-streaming` | Kafka + Bronze streaming |
| 06 | `feature/06-silver` | Transformações + integração |
| 07 | `feature/07-gold` | Agregações analíticas |
| 08 | `feature/08-validacao` | Validação E2E + FinOps |
| 09 | `feature/09-entrega` | README, diagrama, docs finais |

## Regras de commit

- **Um commit por tarefa lógica** do plano de fase
- Mensagens em **português**, prefixo convencional: `feat`, `fix`, `docs`, `test`, `chore`
- Formato: `tipo(escopo): descrição curta do porquê`
- Nunca commitar `.env`, `.env.viewer` ou credenciais

### Exemplos do histórico deste projeto

```
feat(07-01): implementa camada Gold com visoes analiticas e scripts Athena
fix(07): deduplica alunos por id_aluno+ano para contagem Gold correta
test(validation): adiciona script de validacao end-to-end
docs(finops): documenta otimizacoes e estimativa de custos
feat(aws): publica jobs Glue na AWS e usuario viewer somente leitura
```

## Pull Requests

Cada PR deve conter:

```markdown
## O que mudou
- Lista objetiva das alterações

## Por que
- Justificativa em relação ao requisito do Tech Challenge

## Como testar
- Comandos de validação (ex.: validar_pipeline.py, pytest)
- Queries Athena de verificação
```

## Verificar histórico

```bash
git log --oneline
# Projeto com 25+ commits atômicos em português
```

## Usuário viewer (validação)

Para permitir que avaliadores consultem dados sem permissão de escrita:

```powershell
python scripts/provisionar_usuario_viewer.py
```

Credenciais geradas localmente em `.env.viewer` (não versionado).
