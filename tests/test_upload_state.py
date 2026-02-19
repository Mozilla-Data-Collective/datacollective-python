from pathlib import Path

from datacollective.models import UploadPart
from datacollective.uploads import UploadState, load_upload_state, save_upload_state


def test_upload_state_round_trip(tmp_path: Path) -> None:
    state_path = tmp_path / "upload-state.json"
    state = UploadState(
        submissionId="submission",
        fileUploadId="file-upload",
        uploadId="upload-id",
        fileSize=1024,
        partSize=256,
        filename="dataset.tar.gz",
        mimeType="application/gzip",
        parts=[UploadPart(partNumber=1, etag="etag-1")],
        checksum="abc123",
    )

    save_upload_state(state_path, state)
    loaded = load_upload_state(state_path)

    assert loaded is not None
    assert loaded.fileUploadId == state.fileUploadId
    assert loaded.parts[0].partNumber == 1
    assert loaded.parts[0].etag == "etag-1"
