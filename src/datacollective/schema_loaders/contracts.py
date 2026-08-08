from __future__ import annotations

import pandas as pd

from datacollective.errors import TaskValidationError
from datacollective.logging_utils import get_logger
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import Strategy

logger = get_logger(__name__)

#: Logical columns a loaded DataFrame must contain for each known task.
TASK_CONTRACTS: dict[str, frozenset[str]] = {
    "ASR": frozenset({"audio_path", "transcription"}),
    "TTS": frozenset({"audio_path", "transcription"}),
    "LLM": frozenset({"text"}),
}


def _validate_task_contract(df: pd.DataFrame, task: str | None) -> None:
    """Check that the loaded DataFrame satisfies the task's column contract.

    Tasks without a contract (e.g. ``OTH``) and schemas without a task are
    accepted as-is.

    Raises:
        TaskValidationError: If contract columns are missing from *df*.
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
        raise TaskValidationError(
            f"Loaded dataset does not satisfy the '{task.upper()}' task contract: "
            f"missing column(s) {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def _validate_declared_contract(schema: DatasetSchema, strategy: Strategy) -> None:
    """Fail fast when the declared column mappings cannot satisfy the task contract.

    Only applies when the task has a contract, the schema declares column
    mappings, and the strategy actually applies those mappings — catching
    misconfigured schemas before expensive per-row file resolution.

    Raises:
        TaskValidationError: If contract columns are absent from the declared
            logical column names.
    """
    if not schema.task or not schema.columns:
        return
    if not _strategy_applies_mappings(schema, strategy):
        return
    contract = TASK_CONTRACTS.get(schema.task.upper())
    if contract is None:
        return

    missing = sorted(column for column in contract if column not in schema.columns)
    if missing:
        raise TaskValidationError(
            f"Schema for task '{schema.task.upper()}' declares column mappings that "
            f"cannot satisfy the task contract: missing logical column(s) {missing}. "
            f"Declared columns: {sorted(schema.columns)}"
        )


def _strategy_applies_mappings(schema: DatasetSchema, strategy: Strategy) -> bool:
    if strategy == Strategy.GLOB:
        return False
    if strategy == Strategy.PAIRED_GLOB:
        # Only the JSON-sidecar variant applies column mappings
        return (schema.format or "").casefold() == "json"
    return True
