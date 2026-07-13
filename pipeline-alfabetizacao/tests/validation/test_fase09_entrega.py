"""Testes da Fase 09 — documentação e entrega."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SECOES_README = [
    "Contexto do problema",
    "Desafio educacional",
    "Arquitetura proposta",
    "Diagrama da pipeline",
    "Fluxo de dados",
    "Tecnologias utilizadas",
    "Decisões arquiteturais",
    "Monitoramento",
    "FinOps",
    "Aplicação em IA",
    "Estrutura do repositório",
    "Como executar",
    "Validação",
    "Equipe e vídeo",
]


def test_artefatos_entrega_existem() -> None:
    assert (ROOT / "README.md").exists()
    assert (ROOT / "docs" / "diagrama-pipeline.png").exists()
    assert (ROOT / "docs" / "git-workflow.md").exists()
    assert (ROOT / "docs" / "arquitetura.md").exists()
    assert (ROOT / "docs" / "finops.md").exists()


def test_readme_cobre_14_secoes() -> None:
    conteudo = (ROOT / "README.md").read_text(encoding="utf-8")
    for secao in SECOES_README:
        assert secao in conteudo, f"Seção ausente: {secao}"


def test_readme_referencia_docs_internos() -> None:
    conteudo = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "docs/finops.md" in conteudo
    assert "docs/arquitetura.md" in conteudo
    assert "docs/diagrama-pipeline.png" in conteudo
    assert "docs/git-workflow.md" in conteudo


def test_readme_documenta_ia() -> None:
    conteudo = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Predição" in conteudo or "predição" in conteudo
    assert "desigualdade" in conteudo.lower()
    assert "políticas públicas" in conteudo.lower()


def test_diagrama_png_valido() -> None:
    png = ROOT / "docs" / "diagrama-pipeline.png"
    header = png.read_bytes()[:8]
    assert header[:4] == b"\x89PNG", "Arquivo não é PNG válido"
    assert png.stat().st_size > 10_000, "Diagrama muito pequeno — provavelmente corrompido"


def test_git_workflow_documentado() -> None:
    conteudo = (ROOT / "docs" / "git-workflow.md").read_text(encoding="utf-8")
    assert "commit" in conteudo.lower()
    assert "pull request" in conteudo.lower() or "PR" in conteudo
    assert "feature/" in conteudo


def test_readme_raiz_aponta_para_pipeline() -> None:
    repo_root = ROOT.parent
    readme = (repo_root / "README.md").read_text(encoding="utf-8")
    assert "pipeline-alfabetizacao" in readme
    assert "README.md" in readme


def test_historico_git_portugues() -> None:
    result = subprocess.run(
        ["git", "log", "--oneline", "-30"],
        cwd=ROOT.parent,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    linhas = [l for l in result.stdout.strip().splitlines() if l]
    assert len(linhas) >= 15, "Histórico deve ter 15+ commits"
    portugues = sum(
        1
        for l in linhas
        if re.search(
            r"feat|fix|docs|test|chore|implementa|adiciona|corrige|documenta",
            l,
            re.I,
        )
    )
    assert portugues >= 10, "Maioria dos commits deve usar mensagens em português"
