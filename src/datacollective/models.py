from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Task(str, Enum):
    """Valid ML task types for a dataset submission."""

    NA = "N/A"
    NLP = "NLP"
    ASR = "ASR"
    LI = "LI"
    TTS = "TTS"
    MT = "MT"
    LM = "LM"
    LLM = "LLM"
    NLU = "NLU"
    NLG = "NLG"
    CALL = "CALL"
    RAG = "RAG"
    CV = "CV"
    ML = "ML"
    OTHER = "Other"


class NonEmptyStrModel(BaseModel):
    """Base model that trims string fields and rejects empty values."""

    model_config = ConfigDict(extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def _non_empty_strings(cls, value: Any, info: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                raise ValueError(f"`{info.field_name}` must be a non-empty string")
            return trimmed
        return value


class DatasetSubmission(NonEmptyStrModel):
    """
    DatasetSubmission schema aligned with the backend DB representation used
    for draft creation, metadata updates, and final submission.

    Note: Fields are camelCase to match the API payloads.
    """

    id: str | None = None
    organizationId: str | None = None
    createdBy: str | None = None
    status: str | None = None
    slug: str | None = None
    name: str | None = None
    shortDescription: str | None = None
    longDescription: str | None = None
    locale: str | None = None
    task: Task | None = None
    format: str | None = None
    licenseAbbreviation: str | None = None
    license: str | None = None
    licenseUrl: str | None = None
    other: str | None = None
    restrictions: str | None = None
    forbiddenUsage: str | None = None
    additionalConditions: str | None = None
    pointOfContactFullName: str | None = None
    pointOfContactEmail: str | None = None
    fundedByFullName: str | None = None
    fundedByEmail: str | None = None
    legalContactFullName: str | None = None
    legalContactEmail: str | None = None
    createdByFullName: str | None = None
    createdByEmail: str | None = None
    intendedUsage: str | None = None
    ethicalReviewProcess: str | None = None
    fileUploadId: str | None = None
    exclusivityOptOut: bool | None = None
    exclusivityOptOutAt: str | None = None
    agreeToSubmit: bool | None = None
    submittedAt: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None


class UploadPart(BaseModel):
    """A single multipart upload part."""

    model_config = ConfigDict(extra="forbid")

    partNumber: int = Field(..., ge=1)
    etag: str
