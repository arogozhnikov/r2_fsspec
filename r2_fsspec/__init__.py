import json
import s3fs
import os
from pathlib import Path

import time
from fsspec.asyn import sync_wrapper
# ConnectTimeoutError is not handled by fsspec,
#  so we add our own logic on top.
# This allows to tolerate transient problems with network.
from botocore.httpsession import ConnectTimeoutError

__version__ = '2025.05.0' # sync with s3-fsspec 


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
    n_reconnect_retries = 6 # handled by this class
    retries = 10 # we overwrite this from parent fsspec for resilience

    def __init__(self, *, prevent_overwrite=True, **kwargs):
        kwargs = {**get_credentials(), **kwargs}
        self.prevent_overwrite = prevent_overwrite
        super().__init__(**kwargs, fixed_upload_size=True)

    async def _call_s3(self, method, *akwarglist, **kwargs):
        if self.prevent_overwrite and method in ("copy_object", "create_multipart_upload", "put_object"):
            full_key = f"{kwargs['Bucket']}/{kwargs['Key']}"
            if await super()._exists(full_key):
                raise WillNotOverwriteExistingFile(full_key)
            
        for retry in range(self.n_reconnect_retries - 1):
            try:
                return await s3fs.S3FileSystem._call_s3(self, method, *akwarglist, **kwargs)
            except ConnectTimeoutError as e:
                print(f'R2_FSSPEC ERROR: errored on {method}\n{e}')
                time.sleep(10 * 2 ** retry) # 10 sec first, each next is twice more
        # last time without try/except
        return await super()._call_s3(method, *akwarglist, **kwargs)

    call_s3 = sync_wrapper(_call_s3)