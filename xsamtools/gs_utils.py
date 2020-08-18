import io
from uuid import uuid4
from typing import Optional

import google.cloud.exceptions
import gs_chunked_io as gscio
from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


def _blob_for_url(url: str, verify_read_access: bool=False) -> gscio.reader.Blob:
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
    if verify_read_access:
        bucket.blob(key).download_to_file(io.BytesIO(), start=0, end=1)
    return bucket.get_blob(key)

def _write_access(bucket_name: str) -> bool:
    blob = gs.get_client().bucket(bucket_name).blob(f"verify-access-{uuid4()}")
    try:
        blob.upload_from_file(io.BytesIO(b""))
    except (google.cloud.exceptions.NotFound, google.cloud.exceptions.Forbidden):
        return False
    blob.delete()
    return True
