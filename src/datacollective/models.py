from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)


class UploadPart(BaseModel):
    """A single multipart upload part."""

    model_config = ConfigDict(extra="forbid")

    partNumber: int = Field(..., ge=1)
    etag: str


class Task(str, Enum):
    """Valid ML task types for a dataset submission."""

    NA = "N/A"
    NLP = "NLP"
    ASR = "ASR"
    LID = "LID"
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
    OTH = "OTH"


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


class Visibility(str, Enum):
    """
    Dataset visibility levels.

    - ``PUBLIC``: visible to everyone, downloadable by everyone.
    - ``RESTRICTED``: visible to everyone, downloadable by your organization and
      approved requesters (see ``autoApproveAccessRequests``).
    - ``PRIVATE``: visible only to your organization, downloadable by your
      organization via the SDK.
    """

    PUBLIC = "public"
    PRIVATE = "private"
    RESTRICTED = "restricted"


class NonEmptyStrModel(BaseModel):
    """Base model that trims string fields and rejects empty values."""

    model_config = ConfigDict(extra="forbid")
    _allow_empty_trimmed_strings: ClassVar[frozenset[str]] = frozenset(
        {"licenseAbbreviation"}
    )

    @field_validator("*", mode="before")
    @classmethod
    def _non_empty_strings(cls, value: Any, info: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, Enum):
            return value
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed:
                if info.field_name in cls._allow_empty_trimmed_strings:
                    return trimmed
                raise ValueError(f"`{info.field_name}` must be a non-empty string")
            return trimmed
        return value


class Dataset(BaseModel):
    """
    Dataset fields shared by the platform's dataset and dataset-submission
    API payloads. Only fields present in *both* payloads belong here; the
    OpenAPI contract test (tests/test_openapi_contract.py) enforces this.

    DatasetDetails inherits this class and is tolerant to new fields that are
    not declared here in order to prevent breaking changes when the API returns new fields.
    DatasetSubmission inherits this class, adds the submission-only datasheet
    fields and overrides the enum-like fields with strict types for validation.

    Note: Fields are camelCase to match the API payloads.
    """

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
    task: str | None = Field(None, description="ML task type.")
    format: str | None = Field(None, description="File format (e.g., `TSV`, `WAV`).")
    licenseAbbreviation: str | None = Field(
        None, description="Abbreviated license name."
    )
    license: str | None = Field(
        None,
        description="Full license name for custom licenses.",
    )
    isPaid: bool | None = Field(
        None,
        description="Whether the dataset is compensated, i.e. has a price. Defaults to `False` on the platform when left unset.",
    )
    basePriceCents: int | None = Field(
        None,
        description=(
            "Price of the dataset in USD cents (e.g. `100_000` is $1,000.00). Required when "
            "`isPaid` is True. The platform validates that the price is within an acceptable "
            "range and rejects the submission otherwise."
        ),
    )
    # Defined by the API and not user-editable
    id: str | None = Field(
        None, description="Unique identifier as returned by the API."
    )
    slug: str | None = Field(
        None,
        description="URL-friendly slug generated from the name. Determined by the API.",
    )
    createdAt: str | None = Field(
        None,
        description="Timestamp when the record was created. Set by the API upon creation.",
    )


class DatasetSubmission(NonEmptyStrModel, Dataset):
    """
    DatasetSubmission schema aligned with the DB representation used
    for draft creation, metadata updates, and final submission.

    Shared datasheet fields come from Dataset. This model adds the fields
    that only exist on submissions and overrides the enum-like ones with
    strict types so user input is validated before it is sent to the API.
    """

    task: Task | None = Field(
        None,
        description="ML task type — must be one of the Task enum values listed in api.md.",
    )
    licenseAbbreviation: License | str | None = Field(
        None,
        description="Either one of the predefined License enum values or, optionally, a custom abbreviated license name.",
    )
    visibility: Visibility | None = Field(
        None,
        description="Dataset visibility: `public`, `private`, or `restricted`.",
    )
    # Submission-specific datasheet fields defined by the user
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
    pointOfContactEmail: str | None = Field(None, description="Primary contact email.")
    fundedByFullName: str | None = Field(None, description="Funder's name.")
    fundedByEmail: str | None = Field(None, description="Funder's email.")
    legalContactFullName: str | None = Field(None, description="Legal contact name.")
    legalContactEmail: str | None = Field(None, description="Legal contact email.")
    intendedUsage: str | None = Field(None, description="Intended use of the dataset.")
    ethicalReviewProcess: str | None = Field(
        None, description="Description of ethical review conducted."
    )
    showContactInfo: bool | None = Field(
        None,
        description="Whether to publicly display the dataset contact information.",
    )
    showComplianceAttributes: bool | None = Field(
        None,
        description="Whether to publicly display the dataset's compliance attributes.",
    )
    # Submission-specific fields defined by the user
    createdByFullName: str | None = Field(None, description="Creator's name.")
    createdByEmail: str | None = Field(None, description="Creator's email.")
    exclusivityOptOut: bool | None = Field(
        None,
        description="True if dataset is non-exclusive; False if hosted exclusively on Mozilla Data Collective (see https://mozilladatacollective.com/terms/providers#appendix-1).",
    )
    agreeToSubmit: bool | None = Field(
        None,
        description="You confirm that you have the right to submit this dataset and that all information provided in the datasheet is accurate. Required to be True to complete the submission process",
    )
    autoApproveAccessRequests: bool | None = Field(
        None,
        description=(
            "Only applies to `restricted` datasets. When True, the platform grants every "
            "access request automatically as soon as it is made, instead of leaving it "
            "pending for you to review; the requester's email is shared with you and you "
            "can still revoke access later. Defaults to False on the platform when left unset."
        ),
    )
    # Submission-specific fields defined by the API and not user-editable
    organizationId: str | None = Field(
        None,
        description="Identifier for the organization that owns the dataset.",
    )
    createdBy: str | None = Field(
        None, description="Identifier for the user who created the submission."
    )
    status: str | None = Field(
        None,
        description="Current status of the submission (e.g., 'draft', 'submitted'). Determined by the API.",
    )
    fileUploadId: str | None = Field(
        None,
        description="Identifier for the associated file upload, if any. Generated by the API when a file is uploaded.",
    )
    sampleFileReferenceId: str | None = Field(
        None,
        description="Identifier for the associated sample file, if any. Generated by the API when a sample file is uploaded.",
    )
    submittedAt: str | None = Field(
        None,
        description="Timestamp when the submission was finalized and submitted. Set by the API upon submission.",
    )

    updatedAt: str | None = Field(
        None,
        description="Timestamp when the record was last updated. Updated by the API on changes.",
    )

    @model_validator(mode="after")
    def _validate_license_details(self) -> DatasetSubmission:
        has_custom_license_abbreviation = (
            self.licenseAbbreviation is not None
            and not isinstance(self.licenseAbbreviation, License)
        )
        requires_license_name = (
            has_custom_license_abbreviation or self.licenseUrl is not None
        )
        if requires_license_name and self.license is None:
            raise ValueError(
                "`license` is required when providing a custom `licenseAbbreviation` or `licenseUrl`"
            )
        return self

    @model_validator(mode="after")
    def _validate_pricing(self) -> DatasetSubmission:
        if self.isPaid and self.basePriceCents is None:
            raise ValueError(
                "`basePriceCents` is required when `isPaid` is True. The platform only "
                "accepts prices within its allowed range, in USD cents"
            )
        if self.basePriceCents is not None and not self.isPaid:
            raise ValueError(
                "`isPaid` must be True when providing `basePriceCents`, "
                "otherwise the dataset stays uncompensated and the price is ignored"
            )
        return self

    @model_validator(mode="after")
    def _validate_auto_approve_access_requests(self) -> DatasetSubmission:
        if (
            self.autoApproveAccessRequests
            and self.visibility is not None
            and self.visibility != Visibility.RESTRICTED
        ):
            raise ValueError(
                "`autoApproveAccessRequests` only applies to `restricted` datasets, "
                f"the only visibility with access requests; got `{self.visibility.value}`"
            )
        return self

    @computed_field(  # type: ignore[prop-decorator]
        description="Currency for `basePriceCents`. Always `usd`, the only currency the platform currently supports."
    )
    @property
    def currency(self) -> str | None:
        return "usd" if self.isPaid else None


class DatasetDetails(Dataset):
    """
    Dataset details as returned by the MDC API (read model).

    Tolerant of platform schema changes by design: fields the API adds are
    kept as extra attributes, fields the API removes simply read as None,
    and enum-like fields (`task`) are plain strings so new platform values
    don't fail validation. Only `id` is required.

    Dict-style access (`details["id"]`, `details.get("checksum")`) is
    supported for backward compatibility with the previous dict return type.
    """

    model_config = ConfigDict(extra="allow")

    id: str = Field(..., description="Unique identifier of the dataset.")
    filename: str | None = Field(
        None, description="Archive filename of the current dataset file version."
    )
    checksum: str | None = Field(
        None, description="Checksum of the current dataset file version."
    )

    def __contains__(self, key: object) -> bool:
        # Mirrors the previous dict semantics: only keys the API actually
        # returned are "present", even though declared fields always exist
        # as attributes (defaulting to None).
        return isinstance(key, str) and key in self.model_fields_set

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise KeyError(key)
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


FINAL_SUBMISSION_REQUIRED_FIELDS = (
    "name",
    "longDescription",
    "task",
    "locale",
    "format",
    "restrictions",
    "forbiddenUsage",
    "pointOfContactFullName",
    "pointOfContactEmail",
    "showContactInfo",
    "visibility",
)
FINAL_SUBMISSION_LOCAL_FIELDS = set(FINAL_SUBMISSION_REQUIRED_FIELDS) | {
    "licenseAbbreviation",
    "license",
    "fileUploadId",
}
DRAFT_FIELDS = {"name"}
UPDATE_FIELDS = {
    "name",
    "shortDescription",
    "longDescription",
    "locale",
    "task",
    "format",
    "licenseAbbreviation",
    "license",
    "licenseUrl",
    "other",
    "restrictions",
    "forbiddenUsage",
    "additionalConditions",
    "pointOfContactFullName",
    "pointOfContactEmail",
    "fundedByFullName",
    "fundedByEmail",
    "legalContactFullName",
    "legalContactEmail",
    "createdByFullName",
    "createdByEmail",
    "intendedUsage",
    "ethicalReviewProcess",
    "showContactInfo",
    "showComplianceAttributes",
    "visibility",
    "exclusivityOptOut",
    "autoApproveAccessRequests",
    "isPaid",
    "basePriceCents",
    "currency",
}
SUBMIT_FIELDS = {"agreeToSubmit"}


def _ensure_submission_model(submission: DatasetSubmission) -> DatasetSubmission:
    if not isinstance(submission, DatasetSubmission):
        raise TypeError("`submission` must be a DatasetSubmission model")
    return submission


def _payload_for_fields(
    submission: DatasetSubmission, allowed_fields: set[str]
) -> dict[str, Any]:
    data = submission.model_dump(mode="json", exclude_none=True)
    payload = {key: value for key, value in data.items() if key in allowed_fields}

    if "licenseAbbreviation" in allowed_fields and isinstance(
        submission.licenseAbbreviation, License
    ):
        payload["licenseAbbreviation"] = submission.licenseAbbreviation.value
        # Remove custom license fields if a predefined license is used
        payload.pop("license", None)
        payload.pop("licenseUrl", None)

    return payload


def _build_final_submission_error(
    missing_items: list[str], *, missing_file_upload_id: bool
) -> str:
    message = (
        "Cannot submit dataset. Missing required fields for final submission: "
        f"{', '.join(missing_items)}. Please update your DatasetSubmission model "
        f"with the appropriate fields."
    )
    if missing_file_upload_id:
        message += " Upload the dataset file before submitting."
    return message


def _validate_final_submission_fields(
    submission: DatasetSubmission, *, require_file_upload_id: bool
) -> None:
    missing_items: list[str] = []

    for field_name in FINAL_SUBMISSION_REQUIRED_FIELDS:
        if getattr(submission, field_name) is None:
            missing_items.append(f"`{field_name}`")

    if not submission.licenseAbbreviation and not submission.license:
        missing_items.append("either `licenseAbbreviation` or `license`")

    if submission.agreeToSubmit is not True:
        missing_items.append("`agreeToSubmit=True`")

    missing_file_upload_id = require_file_upload_id and submission.fileUploadId is None
    if missing_file_upload_id:
        missing_items.append("`fileUploadId`")

    if missing_items:
        raise ValueError(
            _build_final_submission_error(
                missing_items,
                missing_file_upload_id=missing_file_upload_id,
            )
        )


def _should_validate_local_final_submission(
    submission: DatasetSubmission,
) -> bool:
    return bool(submission.model_fields_set & FINAL_SUBMISSION_LOCAL_FIELDS)


def _require_archive_filename(details: DatasetDetails) -> str:
    if not details.filename:
        raise RuntimeError(
            f"Dataset '{details.id}' details did not include an archive filename."
        )
    return details.filename
