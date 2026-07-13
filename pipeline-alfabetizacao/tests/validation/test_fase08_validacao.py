"""Testes da Fase 08 — validação end-to-end e FinOps."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_modulos_fase08_existem() -> None:
    assert (ROOT / "tests" / "validation" / "validar_pipeline.py").exists()
    assert (ROOT / "docs" / "finops.md").exists()
    assert (ROOT / "scripts" / "estimar_custos.sh").exists()


def test_finops_documentado() -> None:
    conteudo = (ROOT / "docs" / "finops.md").read_text(encoding="utf-8")
    assert "Decisões de otimização" in conteudo
    assert "Estimativa mensal" in conteudo
    assert "Parquet" in conteudo
    assert "Particionamento" in conteudo
    assert "$15" in conteudo or "15" in conteudo


def test_validar_pipeline_executavel() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "tests" / "validation" / "validar_pipeline.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "[DQ] PASS" in result.stdout
    assert "Resultado:" in result.stdout
