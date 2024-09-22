import json
import s3fs
import os
from pathlib import Path


__version__ = '0.0.1'


def get_credentials() -> dict:
    if "R2_FSSPEC_CREDENTIALS" not in os.environ:
        return {}

    credentials_path = Path(os.environ["R2_FSSPEC_CREDENTIALS"])
    assert credentials_path.is_file(), credentials_path

    r2_credentials = json.loads(credentials_path.read_text())

    return {
        "endpoint_url": r2_credentials["endpoint_url"],
        "key": r2_credentials["access_key"],
        "secret": r2_credentials["secret_key"],
        "client_kwargs": dict(region_name=r2_credentials.get("region_name", "enam")),
    }




class R2FileSystem(s3fs.S3FileSystem):
    protocol = "r2"

    def __init__(self, **kwargs):
        # see s3fs.S3FileSystem for documented parameters
        # allows explicitly overwriting any found auth parameters,
        # but demands fixed_upload_size=True, because that's a requirement for R2.
        kwargs = {**get_credentials(), **kwargs}
        super().__init__(**kwargs, fixed_upload_size=True)
        