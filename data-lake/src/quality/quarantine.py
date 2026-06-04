"""Quarantine — stores bad records and generates quality reports."""
from __future__ import annotations

import datetime
import json
from collections import defaultdict
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class Quarantine:
    """Stores records that failed quality checks.

    Records are kept in-memory and optionally persisted to a Delta table
    (via the delta_client if provided) under ``bronze/quarantine/``.
    """

    def __init__(self, delta_client=None, quarantine_path: str | None = None) -> None:
        self._client = delta_client
        self._path = quarantine_path
        # In-memory store: {record_type -> list[dict]}
        self._store: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._report: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "quarantined": 0})

    # ------------------------------------------------------------------
    # Store / retrieve
    # ------------------------------------------------------------------
    def store(self, record_type: str, record: dict[str, Any], reason: str) -> None:
        entry = {
            "record_type": record_type,
            "reason": reason,
            "quarantined_at": datetime.datetime.utcnow().isoformat(),
            "record": json.dumps(record),
        }
        self._store[record_type].append(entry)
        self._report[record_type]["quarantined"] += 1
        logger.warning("Record quarantined", record_type=record_type, reason=reason)

        if self._client and self._path:
            try:
                import pandas as pd

                df = pd.DataFrame([entry])
                self._client.write(
                    f"{self._path}/{record_type}",
                    df,
                    mode="append",
                    partition_by=None,
                )
            except Exception as exc:
                logger.error("Failed to persist quarantine record", error=str(exc))

    def get_quarantined(self, record_type: str | None = None) -> list[dict[str, Any]]:
        if record_type:
            return list(self._store.get(record_type, []))
        return [entry for entries in self._store.values() for entry in entries]

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def record_total(self, record_type: str) -> None:
        """Signal that one more record of *record_type* was seen."""
        self._report[record_type]["total"] += 1

    def quality_report(self) -> dict[str, Any]:
        report: dict[str, Any] = {}
        for rtype, stats in self._report.items():
            total = stats["total"]
            quarantined = stats["quarantined"]
            rate = quarantined / total if total else 0.0
            report[rtype] = {
                "total": total,
                "quarantined": quarantined,
                "pass_rate": round(1.0 - rate, 4),
            }
        return report

    def summary(self) -> dict[str, int]:
        return {rtype: len(entries) for rtype, entries in self._store.items()}
