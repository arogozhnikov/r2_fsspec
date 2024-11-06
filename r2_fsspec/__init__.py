import json
import s3fs
import os
from pathlib import Path

from fsspec.asyn import sync_wrapper
__version__ = '2024.10.0' # in sync with s3fs version


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


class WillNotOverwriteExistingFile(RuntimeError):
    pass



class R2FileSystem(s3fs.S3FileSystem):
    protocol = "r2"

    def __init__(self, *, prevent_overwrite=True, **kwargs):
        # see s3fs.S3FileSystem for documented parameters
        # allows explicitly overwriting any found auth parameters,
        # but demands fixed_upload_size=True, because that's a requirement for R2.
        kwargs = {**get_credentials(), **kwargs}
        self.prevent_overwrite = prevent_overwrite
        super().__init__(**kwargs, fixed_upload_size=True)

    async def _call_s3(self, method, *akwarglist, **kwargs):
        if self.prevent_overwrite and method in ("copy_object", "create_multipart_upload", "put_object"):
            full_key = f"{kwargs['Bucket']}/{kwargs['Key']}"
            if await super()._exists(full_key):
                raise WillNotOverwriteExistingFile(full_key)

        return await super()._call_s3(method, *akwarglist, **kwargs)

    call_s3 = sync_wrapper(_call_s3)