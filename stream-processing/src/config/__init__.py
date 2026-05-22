import sys
from pathlib import Path

# Add repo root to sys.path so finstream_config is importable in local dev.
# In Docker, /app is already on sys.path and finstream_config/ lives there.
_REPO_ROOT = Path(__file__).resolve().parents[3]  # src/config → src → stream-processing → FinStreamAI
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from finstream_config.settings import settings  # noqa: E402

__all__ = ["settings"]
