import pytest
from pydantic import ValidationError

from datacollective.models import DatasetSubmission, Task


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
    )
    assert model.name == "My Dataset"
    assert model.longDescription == "Full description"


def test_submission_accepts_agree_to_submit() -> None:
    model = DatasetSubmission(agreeToSubmit=True)
    assert model.agreeToSubmit is True


def test_submission_accepts_partial_fields() -> None:
    model = DatasetSubmission(
        task=Task.ML,
        licenseAbbreviation="CC-BY-4.0",
        locale="en-US",
    )
    assert model.task == Task.ML
    assert model.licenseAbbreviation == "CC-BY-4.0"
    assert model.locale == "en-US"
    assert model.format is None
    assert model.fileUploadId is None


def test_task_enum_all_values() -> None:
    expected = {
        "N/A",
        "NLP",
        "ASR",
        "LI",
        "TTS",
        "MT",
        "LM",
        "LLM",
        "NLU",
        "NLG",
        "CALL",
        "RAG",
        "CV",
        "ML",
        "Other",
    }
    assert {t.value for t in Task} == expected


def test_task_accepted_by_string_value() -> None:
    model = DatasetSubmission(task="TTS")
    assert model.task == Task.TTS


def test_task_na_value() -> None:
    model = DatasetSubmission(task="N/A")
    assert model.task == Task.NA


def test_task_other_value() -> None:
    model = DatasetSubmission(task="Other")
    assert model.task == Task.OTHER


def test_task_rejects_invalid_value() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(task="invalid-task")


def test_task_rejects_case_variants() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(task="asr")
    with pytest.raises(ValidationError):
        DatasetSubmission(task="Asr")
