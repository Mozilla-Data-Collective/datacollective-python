"""
Contract tests: the SDK's hand-written models against the platform's OpenAPI spec.

The spec snapshot lives at ``tests/fixtures/openapi.json``. You can refresh it with:

    curl -sL https://mozilladatacollective.com/api/openapi.json \\
        | python -m json.tool > tests/fixtures/openapi.json

You can also set ``MDC_OPENAPI_SPEC`` to a file path or URL to test against a different
spec, e.g. the live one.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import requests

from datacollective.download import DownloadPlan
from datacollective.models import (
    DRAFT_FIELDS,
    SUBMIT_FIELDS,
    UPDATE_FIELDS,
    Dataset,
    DatasetDetails,
    DatasetSubmission,
    Task,
    UploadPart,
    Visibility,
)
from datacollective.upload_utils import (
    PresignedPartUrl,
    UploadSession,
    _CompleteUploadPayload,
    _UploadInitiatePayload,
)

SNAPSHOT_PATH = Path(__file__).parent / "fixtures" / "openapi.json"
SPEC_ENV_VAR = "MDC_OPENAPI_SPEC"

# (method, path) pairs the SDK calls. Keep in sync with the URLs built in
# datasets.py, download.py, submissions.py and upload_utils.py.
SDK_OPERATIONS = {
    ("get", "/datasets/{datasetId}"),
    ("post", "/datasets/{datasetId}/download"),
    ("post", "/submissions"),
    ("get", "/submissions/{submissionId}"),
    ("patch", "/submissions/{submissionId}"),
    ("post", "/submissions/{submissionId}"),
    ("post", "/uploads"),
    ("get", "/uploads/{fileUploadId}/parts/{partNumber}"),
    ("post", "/uploads/{fileUploadId}"),
    ("post", "/submissions/{submissionId}/sample"),
    ("get", "/submissions/{submissionId}/sample/{fileUploadId}/parts/{partNumber}"),
    ("post", "/submissions/{submissionId}/sample/{fileUploadId}"),
}


"""
Helper functions
"""


def _load_spec() -> dict[str | bytes, Any]:
    source = os.getenv(SPEC_ENV_VAR, "").strip()
    if source.startswith(("http://", "https://")):
        resp = requests.get(source, timeout=30)
        resp.raise_for_status()
        return dict(resp.json())
    path = Path(source) if source else SNAPSHOT_PATH
    return dict(json.loads(path.read_text()))


def _enum_values(schema: dict[str, Any]) -> set[str]:
    """
    Collect the non-empty string enum values of a property schema.

    The spec expresses "unset" as ``anyOf`` with an ``enum: [""]`` branch and
    nullability as ``null`` inside ``enum``; both are stripped.
    """
    values: set[str] = set()
    for branch in schema.get("anyOf", schema.get("oneOf", [schema])):
        values.update(v for v in branch.get("enum", []) if isinstance(v, str) and v)
    return values


def _own_fields(model: type) -> set[str]:
    """Names of pydantic fields annotated on ``model`` itself, not inherited."""
    return set(model.__annotations__) - {"model_config"}


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    return _load_spec()


@pytest.fixture(scope="module")
def schemas(spec: dict[str, Any]) -> dict[str | bytes, Any]:
    return dict(spec["components"]["schemas"])


@pytest.fixture(scope="module")
def submission_schema(schemas: dict[str, Any]) -> dict[str | bytes, Any]:
    return dict(schemas["SubmissionEnvelope"]["properties"]["submission"])


@pytest.fixture(scope="module")
def update_schema(schemas: dict[str, Any]) -> dict[str | bytes, Any]:
    return dict(schemas["UpdateSubmissionRequest"])


"""
Check SDK - Spec operations / endpoints match
"""


def test_sdk_operations_exist_in_spec(spec: dict[str, Any]) -> None:
    available = {
        (method, path)
        for path, operations in spec["paths"].items()
        for method in operations
    }
    missing = SDK_OPERATIONS - available
    assert not missing, f"SDK calls operations missing from the spec: {sorted(missing)}"


"""
Check enum values
"""


def test_task_enum_matches_spec(
    schemas: dict[str, Any], update_schema: dict[str, Any]
) -> None:
    sdk_values = {task.value for task in Task}
    assert sdk_values == _enum_values(schemas["DatasetSummary"]["properties"]["task"])
    assert sdk_values == _enum_values(update_schema["properties"]["task"])


def test_visibility_enum_matches_update_request(
    update_schema: dict[str, Any], submission_schema: dict[str, Any]
) -> None:
    sdk_values = {visibility.value for visibility in Visibility}
    # The SDK enum validates user input, so it must match what PATCH accepts.
    assert sdk_values == _enum_values(update_schema["properties"]["visibility"])
    # The response enum is a superset of the request enum: `hidden` is a state
    # the platform sets and clients can only observe, never request. Read models
    # keep `visibility` a plain string so such values parse.
    # The SDK enum only covers what PATCH accepts.
    assert sdk_values <= _enum_values(submission_schema["properties"]["visibility"])


"""
Check Upload Submissions
"""


def test_draft_fields_match_create_request(schemas: dict[str, Any]) -> None:
    schema = schemas["CreateSubmissionRequest"]
    assert DRAFT_FIELDS <= set(schema["properties"])
    assert set(schema["required"]) <= DRAFT_FIELDS


def test_update_fields_match_update_request(update_schema: dict[str, Any]) -> None:
    accepted = set(update_schema["properties"])
    # Everything the SDK may send must be accepted by the platform.
    assert UPDATE_FIELDS <= accepted, sorted(UPDATE_FIELDS - accepted)
    # Everything the platform accepts should be settable via the SDK.
    assert accepted <= UPDATE_FIELDS, sorted(accepted - UPDATE_FIELDS)


def test_submit_fields_match_submit_request(schemas: dict[str, Any]) -> None:
    schema = schemas["SubmitForReviewRequest"]
    assert SUBMIT_FIELDS == set(schema["properties"])
    assert set(schema["required"]) <= SUBMIT_FIELDS


"""
Check read models
"""


def test_shared_dataset_fields_exist_in_spec(
    schemas: dict[str, Any], submission_schema: dict[str, Any]
) -> None:
    # `Dataset` documents itself as the fields shared by dataset and
    # submission payloads, so each must appear in at least one of them.
    known = set(schemas["DatasetSummary"]["properties"]) | set(
        submission_schema["properties"]
    )
    missing = set(Dataset.model_fields) - known
    assert not missing, sorted(missing)


def test_submission_fields_exist_in_spec(submission_schema: dict[str, Any]) -> None:
    declared = _own_fields(DatasetSubmission)
    missing = declared - set(submission_schema["properties"])
    assert not missing, sorted(missing)


def test_dataset_details_fields_exist_in_spec(schemas: dict[str, Any]) -> None:
    schema = schemas["DatasetSummary"]
    declared = _own_fields(DatasetDetails)
    missing = declared - set(schema["properties"])
    assert not missing, sorted(missing)
    # `id` is required on the SDK model and `filename` raises when absent.
    assert {"id", "filename"} <= set(schema["required"])


"""
Check download session spec
"""


def test_download_session_fields(schemas: dict[str, Any]) -> None:
    schema = schemas["DownloadSession"]
    # download._build_download_plan reads these keys and requires the first two.
    assert {"downloadUrl", "sizeBytes"} <= set(schema["required"])
    assert "checksum" in schema["properties"]
    assert "size_bytes" in DownloadPlan.model_fields


"""
Check upload session spec
"""


def test_upload_initiate_payload_matches_spec(schemas: dict[str, Any]) -> None:
    schema = schemas["InitiateFileUploadRequest"]
    sent = set(_UploadInitiatePayload.model_fields)
    assert sent <= set(schema["properties"]), sorted(sent - set(schema["properties"]))
    assert set(schema["required"]) <= sent


def test_upload_initiate_response_fields(schemas: dict[str, Any]) -> None:
    schema = schemas["CreateFileUpload"]
    # upload_utils._initiate_upload copies these into UploadSession.
    assert {"fileUploadId", "uploadId"} <= set(schema["required"])
    assert {"fileUploadId", "uploadId"} <= set(UploadSession.model_fields)


def test_presigned_part_response_fields(schemas: dict[str, Any]) -> None:
    schema = schemas["FileUploadPartUrl"]
    assert {"url", "partNumber"} <= set(schema["required"])
    assert set(PresignedPartUrl.model_fields) <= set(schema["properties"])


def test_upload_complete_payload_matches_spec(schemas: dict[str, Any]) -> None:
    schema = schemas["CompleteFileUploadRequest"]
    # `fileUploadId` goes in the URL, not the body.
    sent = set(_CompleteUploadPayload.model_fields) - {"fileUploadId"}
    assert sent == set(schema["properties"]), sorted(sent ^ set(schema["properties"]))
    part_schema = schema["properties"]["parts"]["items"]
    assert set(UploadPart.model_fields) == set(part_schema["properties"])
    assert set(part_schema["required"]) <= set(UploadPart.model_fields)
