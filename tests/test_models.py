import pytest
from pydantic import ValidationError

from datacollective.models import (
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
)


def test_draft_input_rejects_empty_strings() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmissionDraftInput(name=" ", longDescription="Valid description")
    with pytest.raises(ValidationError):
        DatasetSubmissionDraftInput(name="Dataset", longDescription=" ")


def test_draft_input_strips_whitespace() -> None:
    model = DatasetSubmissionDraftInput(
        name="  My Dataset  ",
        longDescription="  Full description  ",
    )
    assert model.name == "My Dataset"
    assert model.longDescription == "Full description"


def test_submit_input_rejects_empty_strings() -> None:
    base_payload = {
        "shortDescription": "Short description",
        "longDescription": "Full description",
        "locale": "en-US",
        "task": "classification",
        "format": "tar.gz",
        "licenseAbbreviation": "CC-BY",
        "license": "Creative Commons Attribution",
        "licenseUrl": "https://creativecommons.org/licenses/by/4.0/",
        "other": "Additional info",
        "restrictions": "Restrictions",
        "forbiddenUsage": "Forbidden usage",
        "additionalConditions": "Additional conditions",
        "pointOfContactFullName": "Jane Doe",
        "pointOfContactEmail": "jane@example.com",
        "fundedByFullName": "Funder Name",
        "fundedByEmail": "funder@example.com",
        "legalContactFullName": "Legal Name",
        "legalContactEmail": "legal@example.com",
        "createdByFullName": "Creator Name",
        "createdByEmail": "creator@example.com",
        "intendedUsage": "Intended usage",
        "ethicalReviewProcess": "Ethical review",
        "exclusivityOptOut": True,
        "fileUploadId": "cuid",
    }

    payload = dict(base_payload)
    payload["shortDescription"] = " "
    with pytest.raises(ValidationError):
        DatasetSubmissionSubmitInput.model_validate(payload)
