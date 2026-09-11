"""
Contract tests: the SDK's hand-written models against the platform's OpenAPI spec.

The spec snapshot lives at ``tests/fixtures/openapi.json``. Refresh it from production:

    curl -sL https://mozilladatacollective.com/api/openapi.json \\
        | python -m json.tool > tests/fixtures/openapi.json

You can also set ``MDC_OPENAPI_SPEC`` to a file path or URL to test against a different
spec, e.g. the live one. The scheduled ``openapi-contract.yml`` workflow refreshes the
snapshot from production and opens a pull request when it changed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import requests

from datacollective.models import (
    DRAFT_FIELDS,
    SUBMIT_FIELDS,
    UPDATE_FIELDS,
    DatasetDetails,
    DatasetSubmission,
    Task,
    UploadPart,
    Visibility,
)
from datacollective.upload_utils import (
    PresignedPartUrl,
    _CompleteUploadPayload,
    _UploadInitiatePayload,
)

SNAPSHOT_PATH = Path(__file__).parent / "fixtures" / "openapi.json"
SPEC_ENV_VAR = "MDC_OPENAPI_SPEC"
SCHEMA_REF_PREFIX = "#/components/schemas/"

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


def _load_spec() -> dict[str, Any]:
    source = os.getenv(SPEC_ENV_VAR, "").strip()
    if source.startswith(("http://", "https://")):
        resp = requests.get(source, timeout=30)
        resp.raise_for_status()
        return dict(resp.json())
    path = Path(source) if source else SNAPSHOT_PATH
    return dict(json.loads(path.read_text()))


def _resolve(schema: dict[str, Any], schemas: dict[str, Any]) -> dict[str, Any]:
    """Follow ``$ref`` pointers into ``components/schemas`` until a concrete schema."""
    while "$ref" in schema:
        ref = schema["$ref"]
        assert ref.startswith(SCHEMA_REF_PREFIX), f"unsupported $ref: {ref}"
        schema = schemas[ref.removeprefix(SCHEMA_REF_PREFIX)]
    return schema


def _enum_values(schema: dict[str, Any], schemas: dict[str, Any]) -> set[str]:
    """
    Collect the non-empty string enum values of a property schema.

    The spec expresses "unset" as ``anyOf`` with an ``enum: [""]`` branch and
    nullability as ``null`` inside ``enum``; both are stripped. ``$ref`` branches
    are resolved so the test survives the platform extracting a shared enum.
    """
    schema = _resolve(schema, schemas)
    values: set[str] = set()
    for branch in _branches(schema, schemas):
        values.update(v for v in branch.get("enum", []) if isinstance(v, str) and v)
    return values


def _branches(schema: dict[str, Any], schemas: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Flatten a schema into its concrete branches.

    ``anyOf``/``oneOf`` (alternatives) and ``allOf`` (composition) are all
    expanded recursively so callers can inspect ``enum`` or ``properties``
    regardless of how the platform composed the schema.
    """
    schema = _resolve(schema, schemas)
    parts = [
        part for key in ("anyOf", "oneOf", "allOf") for part in schema.get(key, [])
    ]
    if not parts:
        return [schema]
    return [leaf for part in parts for leaf in _branches(part, schemas)]


def _properties(schema: dict[str, Any], schemas: dict[str, Any]) -> set[str]:
    """Property names of a schema, including those contributed via ``allOf``."""
    return {
        name
        for branch in _branches(schema, schemas)
        for name in branch.get("properties", {})
    }


def _required(schema: dict[str, Any]) -> set[str]:
    return set(schema.get("required", []))


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    return _load_spec()


@pytest.fixture(scope="module")
def schemas(spec: dict[str, Any]) -> dict[str, Any]:
    return dict(spec["components"]["schemas"])


@pytest.fixture(scope="module")
def dataset_schema(schemas: dict[str, Any]) -> dict[str, Any]:
    return _resolve(schemas["DatasetSummary"], schemas)


@pytest.fixture(scope="module")
def submission_schema(schemas: dict[str, Any]) -> dict[str, Any]:
    envelope = _resolve(schemas["SubmissionEnvelope"], schemas)
    return _resolve(envelope["properties"]["submission"], schemas)


@pytest.fixture(scope="module")
def mutation_submission_schema(schemas: dict[str, Any]) -> dict[str, Any]:
    """The ``submission`` payload returned by create, update and submit."""
    envelope = _resolve(schemas["SubmissionMutationEnvelope"], schemas)
    return _resolve(envelope["properties"]["submission"], schemas)


@pytest.fixture(scope="module")
def update_schema(schemas: dict[str, Any]) -> dict[str, Any]:
    return _resolve(schemas["UpdateSubmissionRequest"], schemas)


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
    schemas: dict[str, Any],
    dataset_schema: dict[str, Any],
    update_schema: dict[str, Any],
) -> None:
    sdk_values = {task.value for task in Task}
    assert sdk_values == _enum_values(dataset_schema["properties"]["task"], schemas)
    assert sdk_values == _enum_values(update_schema["properties"]["task"], schemas)


def test_visibility_enum_matches_update_request(
    schemas: dict[str, Any],
    update_schema: dict[str, Any],
    submission_schema: dict[str, Any],
) -> None:
    sdk_values = {visibility.value for visibility in Visibility}
    # The SDK enum validates user input, so it must match what PATCH accepts.
    assert sdk_values == _enum_values(
        update_schema["properties"]["visibility"], schemas
    )
    # The response enum is a superset of the request enum: `hidden` is a state
    # the platform sets and clients can only observe, never request. The SDK
    # enum only covers what PATCH accepts.
    assert sdk_values <= _enum_values(
        submission_schema["properties"]["visibility"], schemas
    )


"""
Check Upload Submissions
"""


def test_draft_fields_match_create_request(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["CreateSubmissionRequest"], schemas)
    accepted = _properties(schema, schemas)
    # The SDK deliberately creates drafts with the minimum payload and sends the
    # rest via PATCH, so this check is one-directional: nothing the SDK sends may
    # be rejected, and every required field must be one the SDK sends.
    assert DRAFT_FIELDS <= accepted, sorted(DRAFT_FIELDS - accepted)
    assert _required(schema) <= DRAFT_FIELDS


def test_update_fields_match_update_request(
    schemas: dict[str, Any], update_schema: dict[str, Any]
) -> None:
    accepted = _properties(update_schema, schemas)
    # Everything the SDK may send must be accepted by the platform, and
    # everything the platform accepts should be settable via the SDK.
    assert UPDATE_FIELDS == accepted, sorted(UPDATE_FIELDS ^ accepted)


def test_submit_fields_match_submit_request(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["SubmitForReviewRequest"], schemas)
    accepted = _properties(schema, schemas)
    assert SUBMIT_FIELDS == accepted, sorted(SUBMIT_FIELDS ^ accepted)
    assert _required(schema) <= SUBMIT_FIELDS


"""
Check read models
Each read model is checked against the schema of the payload it parses, so a
declared field the platform never returns (and which would silently read as
None) fails here. Fields shared via `Dataset` must exist in both payloads.
"""


def test_submission_fields_exist_in_spec(
    schemas: dict[str, Any],
    submission_schema: dict[str, Any],
    mutation_submission_schema: dict[str, Any],
) -> None:
    # GET returns SubmissionEnvelope; create/update/submit return
    # SubmissionMutationEnvelope. The SDK's model must exist in both.
    declared = set(DatasetSubmission.model_fields)
    missing = declared - _properties(submission_schema, schemas)
    assert not missing, sorted(missing)
    missing = declared - _properties(mutation_submission_schema, schemas)
    assert not missing, sorted(missing)


def test_mutation_response_carries_submission_id(
    schemas: dict[str, Any], mutation_submission_schema: dict[str, Any]
) -> None:
    # submissions.create_submission_with_upload reads `submission.id` from the
    # create response and raises when it is absent.
    envelope = _resolve(schemas["SubmissionMutationEnvelope"], schemas)
    assert "submission" in _required(envelope)
    assert "id" in _required(mutation_submission_schema)


def test_dataset_details_fields_exist_in_spec(
    schemas: dict[str, Any], dataset_schema: dict[str, Any]
) -> None:
    missing = set(DatasetDetails.model_fields) - _properties(dataset_schema, schemas)
    assert not missing, sorted(missing)
    # `id` is required on the SDK model and `filename` raises when absent.
    assert {"id", "filename"} <= _required(dataset_schema)


"""
Check download session spec
"""


def test_download_session_fields(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["DownloadSession"], schemas)
    # download._build_download_plan reads these keys and requires the first two.
    assert {"downloadUrl", "sizeBytes"} <= _required(schema)
    assert "checksum" in _properties(schema, schemas)


"""
Check upload session spec
"""


def test_upload_initiate_payload_matches_spec(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["InitiateFileUploadRequest"], schemas)
    sent = set(_UploadInitiatePayload.model_fields)
    assert sent <= _properties(schema, schemas), sorted(
        sent - _properties(schema, schemas)
    )
    assert _required(schema) <= sent


def test_upload_initiate_response_fields(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["CreateFileUpload"], schemas)
    # upload_utils._initiate_upload copies these into UploadSession.
    assert {"fileUploadId", "uploadId"} <= _required(schema)


def test_presigned_part_response_fields(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["FileUploadPartUrl"], schemas)
    assert {"url", "partNumber"} <= _required(schema)
    assert set(PresignedPartUrl.model_fields) <= _properties(schema, schemas)


def test_upload_complete_payload_matches_spec(schemas: dict[str, Any]) -> None:
    schema = _resolve(schemas["CompleteFileUploadRequest"], schemas)
    # `fileUploadId` goes in the URL, not the body.
    sent = set(_CompleteUploadPayload.model_fields) - {"fileUploadId"}
    assert sent == _properties(schema, schemas), sorted(
        sent ^ _properties(schema, schemas)
    )
    part_schema = _resolve(schema["properties"]["parts"]["items"], schemas)
    assert set(UploadPart.model_fields) == _properties(part_schema, schemas)
    assert _required(part_schema) <= set(UploadPart.model_fields)
