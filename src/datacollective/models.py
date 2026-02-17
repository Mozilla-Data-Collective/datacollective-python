from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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


class DatasetSubmissionDraftInput(NonEmptyStrModel):
    """Input payload for creating a dataset submission draft."""

    name: str
    longDescription: str


class DatasetSubmissionSubmitInput(NonEmptyStrModel):
    """Input payload for submitting a dataset submission."""

    shortDescription: str
    longDescription: str
    locale: str
    task: str
    format: str
    licenseAbbreviation: str
    license: str
    licenseUrl: str
    other: str
    restrictions: str
    forbiddenUsage: str
    additionalConditions: str
    pointOfContactFullName: str
    pointOfContactEmail: str
    fundedByFullName: str
    fundedByEmail: str
    legalContactFullName: str
    legalContactEmail: str
    createdByFullName: str
    createdByEmail: str
    intendedUsage: str
    ethicalReviewProcess: str
    exclusivityOptOut: bool
    fileUploadId: str | None = None
    status: Literal["submitted"] = "submitted"


class DatasetSubmission(BaseModel):
    """
    DatasetSubmission schema aligned with the backend DB representation.

    Note: Fields are camelCase to match the API payloads.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    organizationId: str | None = None
    createdBy: str | None = None
    status: str | None = None
    slug: str | None = None
    name: str | None = None
    shortDescription: str | None = None
    longDescription: str | None = None
    locale: str | None = None
    task: str | None = None
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
