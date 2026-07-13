"""Deploy completo AWS: assets, jobs Glue e usuário viewer."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    ("provisionar_iam_glue.py", []),
    ("publicar_glue_aws.py", []),
    ("provisionar_jobs_glue.py", []),
    ("provisionar_usuario_viewer.py", []),
    ("registrar_tabelas_bronze_athena.py", []),
    ("registrar_tabelas_silver_athena.py", []),
    ("registrar_tabelas_gold_athena.py", []),
]


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Deploy pipeline na AWS")
    parser.add_argument("--sem-staging", action="store_true")
    parser.add_argument("--executar-smoke", action="store_true", help="Roda smoke test Glue após deploy")
    args = parser.parse_args()

    for script, extra in SCRIPTS:
        cmd = [sys.executable, str(ROOT / "scripts" / script), *extra]
        if script == "publicar_glue_aws.py" and args.sem_staging:
            cmd.append("--sem-staging")
        print(f"\n=== {script} ===")
        subprocess.run(cmd, cwd=ROOT, check=True)

    if args.executar_smoke:
        print("\n=== executar_pipeline_aws.py --modo smoke ===")
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "executar_pipeline_aws.py"), "--modo", "smoke"],
            cwd=ROOT,
            check=True,
        )

    print("\nDeploy AWS concluído.")


if __name__ == "__main__":
    main()
