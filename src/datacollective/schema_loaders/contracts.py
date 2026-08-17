from __future__ import annotations

import warnings

import pandas as pd

from datacollective.errors import TaskValidationWarning
from datacollective.logging_utils import get_logger

logger = get_logger(__name__)

#: Logical columns a loaded DataFrame is expected to contain for each known task.
TASK_CONTRACTS: dict[str, frozenset[str]] = {
    "ASR": frozenset({"audio_path", "transcription"}),
    "TTS": frozenset({"audio_path", "transcription"}),
    "LLM": frozenset({"text"}),
}


def _validate_task_contract(df: pd.DataFrame, task: str | None) -> None:
    """Check that the loaded DataFrame satisfies the task's column contract.

    A violation emits a `TaskValidationWarning` (via :mod:`warnings`, so it is
    visible even when package logging is disabled) — the DataFrame is still
    returned as-is. Tasks without a contract (e.g. ``OTH``) and schemas
    without a task are accepted silently.
    """
    if not task:
        logger.debug("Schema has no task — skipping task contract validation.")
        return
    contract = TASK_CONTRACTS.get(task.upper())
    if contract is None:
        logger.debug(
            f"No contract defined for task '{task}' — skipping task contract validation."
        )
        return

    missing = sorted(column for column in contract if column not in df.columns)
    if missing:
        warnings.warn(
            f"Loaded dataset does not satisfy the '{task.upper()}' task contract: "
            f"missing column(s) {missing}. "
            f"Available columns: {list(df.columns)}",
            TaskValidationWarning,
            stacklevel=2,
        )
