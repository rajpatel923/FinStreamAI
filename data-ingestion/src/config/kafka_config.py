import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from finstream_config.settings import Settings as KafkaConfig  # noqa: E402
from finstream_config.settings import kafka_config  # noqa: E402

__all__ = ["KafkaConfig", "kafka_config"]
