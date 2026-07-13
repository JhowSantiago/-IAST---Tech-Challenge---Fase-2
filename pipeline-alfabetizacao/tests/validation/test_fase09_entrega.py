"""Testes da Fase 09 - documentacao e entrega."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SECOES_README = [
    "Contexto do problema",
    "Desafio educacional",
    "Arquitetura proposta",
    "Diagrama da pipeline",
    "Fluxo de dados",
    "Tecnologias utilizadas",
    "Decisoes arquiteturais",
    "Monitoramento",
    "FinOps",
    "Aplicacao em IA",
    "Estrutura do repositorio",
    "Como executar",
    "Validacao",
    "Equipe e video",
]


def test_artefatos_entrega_existem() -> None:
    assert (REPO_ROOT / "README.md").exists()
    assert (ROOT / "docs" / "diagrama-pipeline.png").exists()
    assert (ROOT / "docs" / "git-workflow.md").exists()
    assert (ROOT / "docs" / "arquitetura.md").exists()
    assert (ROOT / "docs" / "finops.md").exists()
    assert (REPO_ROOT / "Entrega_Tech_Challenge_Fase1_Jonathan.pdf").exists()
    assert (REPO_ROOT / "NPS_Strategic_Mitigation.pptx").exists()


def test_readme_cobre_14_secoes() -> None:
    conteudo = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for secao in SECOES_README:
        assert secao.lower() in conteudo.lower(), f"Secao ausente: {secao}"


def test_readme_referencia_docs_internos() -> None:
    conteudo = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "pipeline-alfabetizacao/docs/finops.md" in conteudo
    assert "pipeline-alfabetizacao/docs/arquitetura.md" in conteudo
    assert "pipeline-alfabetizacao/docs/diagrama-pipeline.png" in conteudo
    assert "pipeline-alfabetizacao/docs/git-workflow.md" in conteudo


def test_readme_referencia_materiais_complementares() -> None:
    conteudo = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "Entrega_Tech_Challenge_Fase1_Jonathan.pdf" in conteudo
    assert "NPS_Strategic_Mitigation.pptx" in conteudo
    assert "Materiais de entrega complementares" in conteudo


def test_readme_documenta_ia() -> None:
    conteudo = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "predicao" in conteudo.lower() or "predição" in conteudo.lower()
    assert "desigualdade" in conteudo.lower()
    assert "politicas publicas" in conteudo.lower() or "políticas públicas" in conteudo.lower()


def test_diagrama_png_valido() -> None:
    png = ROOT / "docs" / "diagrama-pipeline.png"
    header = png.read_bytes()[:8]
    assert header[:4] == b"\x89PNG", "Arquivo nao e PNG valido"
    assert png.stat().st_size > 10_000, "Diagrama muito pequeno"


def test_git_workflow_documentado() -> None:
    conteudo = (ROOT / "docs" / "git-workflow.md").read_text(encoding="utf-8")
    assert "commit" in conteudo.lower()
    assert "pull request" in conteudo.lower() or "PR" in conteudo
    assert "feature/" in conteudo


def test_readme_raiz_e_documentacao_principal() -> None:
    readme_raiz = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    readme_pipeline = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "contexto do problema" in readme_raiz.lower()
    assert "diagrama da pipeline" in readme_raiz.lower()
    assert "README da raiz" in readme_pipeline or "../README.md" in readme_pipeline


def test_gitignore_nao_versionado() -> None:
    result = subprocess.run(
        ["git", "ls-files", ".gitignore"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "", ".gitignore nao deve estar versionado"


def test_historico_git_portugues() -> None:
    result = subprocess.run(
        ["git", "log", "--oneline", "-30"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    linhas = [line for line in result.stdout.strip().splitlines() if line]
    assert len(linhas) >= 15, "Historico deve ter 15+ commits"
    portugues = sum(
        1
        for line in linhas
        if re.search(
            r"feat|fix|docs|test|chore|implementa|adiciona|corrige|documenta",
            line,
            re.I,
        )
    )
    assert portugues >= 10, "Maioria dos commits deve usar mensagens em portugues"
