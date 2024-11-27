# r2_fsspec

Support of r2 storage in fsspec as r2://

Cloudflare's R2 has S3-like interface. 

There are a couple of reasons for existense of this package:

1. use prefix r2://bucket/path/to-object not s3:// 
   - very helpful when using s3 and r2 in parallel

2. this package auto-grabs credentials for r2 from file, pointed by `R2_FSSPEC_CREDENTIALS` env var if specified, otherwise falls back to s3fs to search for credentials.

3. ~~strategy of s3fs upload does not work with R2, so this package is pinned on specific version of modified fsspec.~~ I've upstreamed necessary changes, s3fs now supports R2 directly

4. by default, `r2_fsspec` prevents file overwrites (unlike s3fs and unlike fsspec in general).


This package heavily relies on canonical fsspec/s3fs, 
and patches necessary places, thus it dependes on strictly pinned version of s3fs.

### Usage:

```
pip install git+https://github.com/arogozhnikov/r2_fsspec
```

you don't need to import r2_fsspec directly, fsspec will automatically use it to handle links with 'r2://'
