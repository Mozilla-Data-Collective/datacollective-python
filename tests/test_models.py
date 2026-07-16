import pytest
from pydantic import ValidationError

from datacollective.models import (
    DatasetDetails,
    DatasetSubmission,
    License,
    Task,
    Visibility,
)


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
        license="Mozilla Community Data License",
        licenseAbbreviation="MCDL",
        locale="en-US",
    )
    assert model.task == Task.ML
    assert model.license == "Mozilla Community Data License"
    assert model.licenseAbbreviation == "MCDL"
    assert model.locale == "en-US"
    assert model.format is None
    assert model.fileUploadId is None


def test_task_enum_all_values() -> None:
    expected = {
        "N/A",
        "NLP",
        "ASR",
        "LID",
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
        "OTH",
    }
    assert {t.value for t in Task} == expected


def test_task_accepted_by_string_value() -> None:
    model = DatasetSubmission(task="TTS")
    assert model.task == Task.TTS


def test_task_na_value() -> None:
    model = DatasetSubmission(task="N/A")
    assert model.task == Task.NA


def test_task_other_value() -> None:
    model = DatasetSubmission(task="OTH")
    assert model.task == Task.OTH


def test_task_rejects_invalid_value() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(task="invalid-task")


def test_task_rejects_case_variants() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(task="asr")
    with pytest.raises(ValidationError):
        DatasetSubmission(task="Asr")


def test_predefined_license_is_accepted_by_enum() -> None:
    model = DatasetSubmission(licenseAbbreviation=License.CC_BY_4_0)
    assert model.licenseAbbreviation == License.CC_BY_4_0


def test_custom_license_allows_optional_details() -> None:
    model = DatasetSubmission(
        license="Mozilla Research License",
        licenseAbbreviation="MRL",
        licenseUrl="https://example.com/license",
    )
    assert model.license == "Mozilla Research License"
    assert model.licenseAbbreviation == "MRL"
    assert model.licenseUrl == "https://example.com/license"


def test_license_details_require_license_name() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(licenseAbbreviation="Custom")
    with pytest.raises(ValidationError):
        DatasetSubmission(licenseUrl="https://example.com/license")


def test_empty_license_abbreviation_requires_or_uses_license_name() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(licenseAbbreviation="")

    model = DatasetSubmission(
        licenseAbbreviation="",
        license="Mozilla Research License",
    )
    assert model.licenseAbbreviation == ""
    assert model.license == "Mozilla Research License"


def test_visibility_enum_values() -> None:
    assert {v.value for v in Visibility} == {"public", "private", "restricted"}


def test_visibility_accepted_by_enum_and_string() -> None:
    assert DatasetSubmission(visibility=Visibility.PRIVATE).visibility == (
        Visibility.PRIVATE
    )
    assert DatasetSubmission(visibility="restricted").visibility == (
        Visibility.RESTRICTED
    )


def test_visibility_rejects_invalid_value() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission(visibility="internal")


def test_show_contact_info_accepts_boolean() -> None:
    assert DatasetSubmission(showContactInfo=True).showContactInfo is True
    assert DatasetSubmission(showContactInfo=False).showContactInfo is False


def test_dataset_details_requires_id() -> None:
    with pytest.raises(ValidationError):
        DatasetDetails.model_validate({"filename": "dataset.tar.gz"})


def test_dataset_details_missing_fields_default_to_none() -> None:
    details = DatasetDetails.model_validate({"id": "abc"})
    assert details.filename is None
    assert details.checksum is None
    assert details.name is None


def test_dataset_details_keeps_unknown_fields() -> None:
    details = DatasetDetails.model_validate(
        {"id": "abc", "filename": "dataset.tar.gz", "brandNewField": 123}
    )
    assert details["brandNewField"] == 123
    assert "brandNewField" in details
    assert details.model_dump()["brandNewField"] == 123


def test_dataset_details_accepts_unknown_enum_values() -> None:
    details = DatasetDetails.model_validate(
        {"id": "abc", "visibility": "hidden", "task": "BRAND_NEW_TASK"}
    )
    assert details.visibility == "hidden"
    assert details.task == "BRAND_NEW_TASK"


def test_dataset_details_dict_style_access() -> None:
    details = DatasetDetails.model_validate({"id": "abc", "name": "My dataset"})
    assert details["id"] == "abc"
    assert details["name"] == "My dataset"
    assert details.get("name") == "My dataset"
    # keys the API did not return behave as absent, matching dict semantics,
    # even for declared fields (which still read as None via attribute access)
    assert details.get("checksum", "") == ""
    assert details.get("missing") is None
    assert "id" in details
    assert "checksum" not in details
    assert "missing" not in details
    assert details.checksum is None
    with pytest.raises(KeyError):
        details["missing"]


def test_submission_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        DatasetSubmission.model_validate({"name": "My dataset", "brandNewField": 123})


def test_submission_shared_fields_still_validated() -> None:
    # NonEmptyStrModel validators must apply to fields inherited from Dataset
    with pytest.raises(ValidationError):
        DatasetSubmission(locale="   ")
    assert DatasetSubmission(locale=" en-US ").locale == "en-US"
