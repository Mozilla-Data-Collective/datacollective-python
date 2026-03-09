from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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


class License(str, Enum):
    """List of pre-defined dataset licenses."""

    APACHE_2_0 = "Apache-2.0"
    BSD_3_CLAUSE = "BSD-3-Clause"
    CC_BY_4_0 = "CC-BY-4.0"
    CC_BY_ND_4_0 = "CC-BY-ND-4.0"
    CC_BY_NC_4_0 = "CC-BY-NC-4.0"
    CC_BY_NC_SA_4_0 = "CC-BY-NC-SA-4.0"
    CC_BY_SA_4_0 = "CC-BY-SA-4.0"
    CC_SA_1_0 = "CC-SA-1.0"
    CC0_1_0 = "CC0-1.0"
    EUPL_1_2 = "EUPL-1.2"
    AGPL_3_0 = "AGPL-3.0"
    GFDL_1_3 = "GFDL-1.3"
    GPL_3_0 = "GPL-3.0"
    LGPLLR = "LGPLLR"
    MIT = "MIT"
    MPL_2_0 = "MPL-2.0"
    NLOD_2_0 = "NLOD-2.0"
    NOODL_1_0 = "NOODL-1.0"
    ODC_BY_1_0 = "ODC-By-1.0"
    ODBL_1_0 = "ODbL-1.0"
    OGL_CANADA_2_0 = "OGL-Canada-2.0"
    OGL_UK_3_0 = "OGL-UK-3.0"
    OPUBL_1_0 = "OPUBL-1.0"
    OGDL_TAIWAN_1_0 = "OGDL-Taiwan-1.0"
    UNLICENSE = "Unlicense"


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

    # Defined by the user
    name: str | None = Field(None, description="Name of the dataset.")
    shortDescription: str | None = Field(
        None, description="Brief description of the dataset."
    )
    longDescription: str | None = Field(
        None, description="Detailed description of the dataset."
    )
    locale: str | None = Field(
        None, description="Language/locale code (e.g., `en-US`, `de-DE`)."
    )
    task: Task | None = Field(
        None,
        description="ML task type — must be one of the Task enum values listed in api.md.",
    )
    format: str | None = Field(
        None, description="File format (e.g., `TSV`, `WAV`)."
    )
    licenseAbbreviation: str | None = Field(
        None,
        description="Optional short license name for custom licenses.",
    )
    license: License | str | None = Field(
        None,
        description="Either one of the predefined License enum values or a custom full license name.",
    )
    licenseUrl: str | None = Field(
        None,
        description="Optional URL to the license text for custom licenses.",
    )
    other: str | None = Field(None, description="The datasheet of the dataset.")
    restrictions: str | None = Field(
        None, description="Any restrictions on dataset use."
    )
    forbiddenUsage: str | None = Field(
        None, description="Explicitly forbidden use cases."
    )
    additionalConditions: str | None = Field(
        None, description="Additional conditions for use."
    )
    pointOfContactFullName: str | None = Field(
        None, description="Primary contact name."
    )
    pointOfContactEmail: str | None = Field(
        None, description="Primary contact email."
    )
    fundedByFullName: str | None = Field(None, description="Funder's name.")
    fundedByEmail: str | None = Field(None, description="Funder's email.")
    legalContactFullName: str | None = Field(
        None, description="Legal contact name."
    )
    legalContactEmail: str | None = Field(
        None, description="Legal contact email."
    )
    createdByFullName: str | None = Field(None, description="Creator's name.")
    createdByEmail: str | None = Field(None, description="Creator's email.")
    intendedUsage: str | None = Field(
        None, description="Intended use of the dataset."
    )
    ethicalReviewProcess: str | None = Field(
        None, description="Description of ethical review conducted."
    )
    exclusivityOptOut: bool | None = Field(
        None,
        description="True if dataset is non-exclusive; False if hosted exclusively on Mozilla Data Collective (see https://datacollective.mozillafoundation.org/terms/providers#appendix-1).",
    )
    agreeToSubmit: bool | None = Field(
        None,
        description="You confirm that you have the right to submit this dataset and that all information provided in the datasheet is accurate. Required to be True to complete the submission process",
    )
    # Defined by the API and not user-editable
    id: str | None = Field(None, description="Unique identifier for the submission as returned by the API.")
    organizationId: str | None = Field(None, description="Identifier for the organization associated with the submission.")
    createdBy: str | None = Field(None, description="Identifier for the user who created the submission.")
    status: str | None = Field(None, description="Current status of the submission (e.g., 'draft', 'submitted'). Determined by the API.")
    slug: str | None = Field(None, description="URL-friendly slug for the submission, generated from the name. Determined by the API.")
    fileUploadId: str | None = Field(None, description="Identifier for the associated file upload, if any. Generated by the API when a file is uploaded.")
    exclusivityOptOutAt: str | None = Field(
        None, description="Timestamp when exclusivity opt-out was set, if applicable."
    )
    submittedAt: str | None = Field(
        None, description="Timestamp when the submission was finalized and submitted. Set by the API upon submission."
    )
    createdAt: str | None = Field(
        None, description="Timestamp when the submission was created. Set by the API upon creation."
    )
    updatedAt: str | None = Field(
        None, description="Timestamp when the submission was last updated. Updated by the API on changes."
    )

    @field_validator("license", mode="after")
    @classmethod
    def _normalize_license(cls, value: License | str | None) -> License | str | None:
        if isinstance(value, str):
            try:
                return License(value)
            except ValueError:
                return value
        return value

    @model_validator(mode="after")
    def _validate_license_fields(self) -> "DatasetSubmission":
        has_license_details = (
            self.licenseAbbreviation is not None or self.licenseUrl is not None
        )

        if isinstance(self.license, License):
            if has_license_details:
                raise ValueError(
                    "`licenseUrl` and `licenseAbbreviation` must be omitted when `license` is one of the predefined License values"
                )
            return self

        if self.license is None and has_license_details:
            raise ValueError(
                "`license` must be provided when `licenseUrl` or `licenseAbbreviation` is set"
            )

        return self


class UploadPart(BaseModel):
    """A single multipart upload part."""

    model_config = ConfigDict(extra="forbid")

    partNumber: int = Field(..., ge=1)
    etag: str
