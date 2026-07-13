"""Bootstrap de imports para jobs AWS Glue (--extra-py-files)."""

from __future__ import annotations

import sys
from pathlib import Path


def setup_glue_path() -> None:
    """Garante que o pacote `src` seja importável no runtime Glue e local."""
    try:
        import src  # noqa: F401
    except ImportError:
        root = Path(__file__).resolve().parents[1]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
