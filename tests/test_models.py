import pytest
from pydantic import ValidationError

from datacollective.models import (
    DatasetSubmissionDraftInput,
    DatasetSubmissionSubmitInput,
    DatasetSubmissionUpdateInput,
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


def test_submit_input_accepts_agree_to_submit() -> None:
    model = DatasetSubmissionSubmitInput(agreeToSubmit=True)
    assert model.agreeToSubmit is True


def test_submit_input_accepts_false() -> None:
    model = DatasetSubmissionSubmitInput(agreeToSubmit=False)
    assert model.agreeToSubmit is False


def test_update_input_rejects_empty_strings() -> None:
    payload = {
        "task": "ML",
        "locale": " ",
    }
    with pytest.raises(ValidationError):
        DatasetSubmissionUpdateInput.model_validate(payload)


def test_update_input_accepts_partial_fields() -> None:
    model = DatasetSubmissionUpdateInput(
        task="ML",
        licenseAbbreviation="CC-BY-4.0",
        locale="en-US",
    )
    assert model.task == "ML"
    assert model.licenseAbbreviation == "CC-BY-4.0"
    assert model.locale == "en-US"
    assert model.format is None
    assert model.fileUploadId is None


def test_update_input_strips_whitespace() -> None:
    model = DatasetSubmissionUpdateInput(
        task="  ML  ",
        locale="  en-US  ",
    )
    assert model.task == "ML"
    assert model.locale == "en-US"
