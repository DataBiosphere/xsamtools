import io
from uuid import uuid4
from typing import Sequence, Optional

import google.cloud.exceptions
import gs_chunked_io as gscio
from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


def _blob_for_url(url: str) -> Optional[gscio.reader.Blob]:
    if url.startswith("gs://"):
        bucket_name, key = url[5:].split("/", 1)
        client = gs.get_client()
        bucket = client.bucket(bucket_name)
    elif url.startswith("drs://"):
        client, info = drs.resolve_drs_for_gs_storage(url)
        drs.enable_requester_pays()
        bucket = client.bucket(info.bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
        key = info.key
    else:
        raise ValueError(f"expected drs:// or gs:// url, not {url}")
    return bucket.get_blob(key)

def _read_access(url: str) -> bool:
    assert url.startswith("gs://") or url.startswith("drs://")
    blob = _blob_for_url(url)
    if blob is None:
        return False
    try:
        blob.download_to_file(io.BytesIO(), start=0, end=1)
        return True
    except google.cloud.exceptions.Forbidden:
        return False

def _write_access(bucket_name: str) -> bool:
    blob = gs.get_client().bucket(bucket_name).blob(f"verify-access-{uuid4()}")
    try:
        blob.upload_from_file(io.BytesIO(b""))
    except (google.cloud.exceptions.NotFound, google.cloud.exceptions.Forbidden):
        return False
    blob.delete()
    return True

def _assert_access(read_paths: Sequence[str], write_paths: Sequence[str]):
    for p in read_paths:
        if p.startswith("gs://") or p.startswith("drs://"):
            assert _read_access(p)
    for p in write_paths:
        if p.startswith("gs://"):
            bucket_name, _ = p[5:].split("/", 1)
            assert _write_access(bucket_name)
