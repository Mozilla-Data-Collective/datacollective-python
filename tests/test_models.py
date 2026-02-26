import pytest
from pydantic import ValidationError

from datacollective.models import DatasetSubmission


def test_submission_rejects_empty_strings() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(name=" ", longDescription="Valid description")
    with pytest.raises(ValidationError):
        DatasetSubmission(name="Dataset", longDescription=" ")
    with pytest.raises(ValidationError):
        DatasetSubmission(locale=" ")


def test_submission_strips_whitespace() -> None:
    model = DatasetSubmission(
        name="  My Dataset  ",
        longDescription="  Full description  ",
        task="  ML  ",
    )
    assert model.name == "My Dataset"
    assert model.longDescription == "Full description"
    assert model.task == "ML"


def test_submission_accepts_agree_to_submit() -> None:
    model = DatasetSubmission(agreeToSubmit=True)
    assert model.agreeToSubmit is True


def test_submission_accepts_partial_fields() -> None:
    model = DatasetSubmission(
        task="ML",
        licenseAbbreviation="CC-BY-4.0",
        locale="en-US",
    )
    assert model.task == "ML"
    assert model.licenseAbbreviation == "CC-BY-4.0"
    assert model.locale == "en-US"
    assert model.format is None
    assert model.fileUploadId is None
