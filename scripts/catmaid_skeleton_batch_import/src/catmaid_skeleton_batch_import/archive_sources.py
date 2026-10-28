"""Validate archive locations and protect temporary download credentials."""

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, unquote, urlsplit

from cryptography.fernet import Fernet, InvalidToken

SOURCE_KEY_SECRET = "skeleton_ingestion_source_key"
SOURCE_KEY_ENV = "SKELETON_INGESTION_SOURCE_KEY"
SOURCE_ENCRYPTED_ENV = "SKELETON_INGESTION_SOURCE_ENCRYPTED"
MAX_SOURCE_URL_LENGTH = 8192
_BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_S3_HOST_RE = re.compile(
    r"(?:(?P<bucket>[a-z0-9][a-z0-9.-]*)\.)?s3"
    r"(?:[.-](?:dualstack\.)?[a-z]{2}(?:-[a-z0-9]+)+-\d+)?"
    r"\.amazonaws\.com(?:\.cn)?"
)


class InvalidArchiveSource(ValueError):
    """An error safe to return without disclosing a signed URL."""


@dataclass(frozen=True)
class PublicS3Object:
    uri: str
    bucket: str
    key: str

    def metadata(self) -> dict[str, str]:
        return {"uri": self.uri, "bucket": self.bucket, "key": self.key}


@dataclass(frozen=True)
class SignedArchiveSource:
    uri: str
    bucket: str
    key: str
    provider: str
    url: str = field(repr=False)

    def metadata(self) -> dict[str, str]:
        return {
            "uri": self.uri,
            "bucket": self.bucket,
            "key": self.key,
            "provider": self.provider,
            "url_sha256": hashlib.sha256(self.url.encode("ascii")).hexdigest(),
        }


def parse_public_s3_uri(uri: str) -> PublicS3Object:
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise InvalidArchiveSource("S3 URI must use the lowercase s3:// scheme")
    if any(ord(c) < 32 or ord(c) == 127 for c in uri):
        raise InvalidArchiveSource("S3 URI cannot contain control characters")
    try:
        parsed = urlsplit(uri)
        port = parsed.port
    except ValueError:
        raise InvalidArchiveSource("Invalid S3 URI") from None
    if parsed.username is not None or parsed.password is not None or port is not None:
        raise InvalidArchiveSource("S3 URI cannot contain credentials or a port")
    if parsed.query or parsed.fragment:
        raise InvalidArchiveSource("S3 URI cannot contain a query or fragment")
    bucket = parsed.netloc
    if (
        not _BUCKET_RE.fullmatch(bucket)
        or ".." in bucket
        or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket)
    ):
        raise InvalidArchiveSource("S3 URI contains an invalid AWS bucket name")
    key = parsed.path.removeprefix("/")
    if not key:
        raise InvalidArchiveSource("S3 URI must identify an object key")
    if len(key.encode("utf-8")) > 1024:
        raise InvalidArchiveSource("S3 object key exceeds 1024 UTF-8 bytes")
    return PublicS3Object(uri=uri, bucket=bucket, key=key)


def parse_archive_source(value: str) -> PublicS3Object | SignedArchiveSource:
    if not isinstance(value, str) or not value or len(value) > MAX_SOURCE_URL_LENGTH:
        raise InvalidArchiveSource("Archive source is missing or too long")
    if value.startswith("s3://"):
        return parse_public_s3_uri(value)
    if any(ord(c) <= 32 or ord(c) >= 127 for c in value) or "\\" in value:
        raise InvalidArchiveSource("Signed URLs must be percent-encoded HTTPS URLs")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise InvalidArchiveSource("Invalid signed archive URL") from None
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
        or "#" in value
    ):
        raise InvalidArchiveSource(
            "Signed URLs require HTTPS without user info or fragments"
        )
    host = parsed.hostname or ""
    aws = _S3_HOST_RE.fullmatch(host)
    if aws:
        provider = "s3"
        bucket = aws.group("bucket")
    elif host == "storage.googleapis.com" or host.endswith(".storage.googleapis.com"):
        provider = "gcs"
        bucket = (
            host.removesuffix(".storage.googleapis.com")
            if host != "storage.googleapis.com"
            else None
        )
    else:
        raise InvalidArchiveSource(
            "Signed URLs must use an AWS S3 or GCS storage endpoint"
        )
    path = parsed.path.removeprefix("/")
    if bucket is None:
        bucket, separator, key = path.partition("/")
        if not separator:
            raise InvalidArchiveSource("Signed URL must identify a bucket and object")
    else:
        key = path
    bucket_pattern = (
        _BUCKET_RE
        if provider == "s3"
        else re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]")
    )
    if not bucket_pattern.fullmatch(bucket) or ".." in bucket or not key:
        raise InvalidArchiveSource("Signed URL must identify a valid bucket and object")
    if len(unquote(key).encode("utf-8")) > 1024:
        raise InvalidArchiveSource("Signed URL object key exceeds 1024 UTF-8 bytes")
    try:
        pairs = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=128,
        )
    except ValueError:
        raise InvalidArchiveSource("Invalid signed URL query") from None
    query = dict(pairs)
    if len(query) != len(pairs):
        raise InvalidArchiveSource("Signed URL query cannot repeat parameters")
    prefix = "X-Amz" if provider == "s3" else "X-Goog"
    if f"{prefix}-Signature" in query:
        required = {
            f"{prefix}-{name}"
            for name in (
                "Algorithm",
                "Credential",
                "Date",
                "Expires",
                "SignedHeaders",
                "Signature",
            )
        }
        if query.get(f"{prefix}-SignedHeaders") != "host":
            raise InvalidArchiveSource(
                "Signed GET URLs must require only the host header"
            )
    else:
        required = {
            "Signature",
            "Expires",
            "AWSAccessKeyId" if provider == "s3" else "GoogleAccessId",
        }
    if not required.issubset(query) or any(not query[name] for name in required):
        raise InvalidArchiveSource(
            "URL must contain an AWS S3 presign or GCS signature"
        )
    uri = value.split("?", 1)[0]
    if len(uri) > 2048:
        raise InvalidArchiveSource("Archive object location exceeds 2048 characters")
    return SignedArchiveSource(
        uri=uri, bucket=bucket, key=key, provider=provider, url=value
    )


def redact_archive_url(value: str) -> str:
    return value.split("?", 1)[0].split("#", 1)[0]


def _cipher(key: str) -> Fernet:
    if not key or len(key) < 20:
        raise InvalidArchiveSource("Signed URL encryption key is missing or invalid")
    return Fernet(
        base64.urlsafe_b64encode(hashlib.sha256(key.encode("utf-8")).digest())
    )


def encrypt_source_url(url: str, ingestion_id: str, key: str) -> str:
    payload = json.dumps(
        {"version": 1, "ingestion_id": ingestion_id, "url": url}
    ).encode("utf-8")
    return _cipher(key).encrypt(payload).decode("ascii")


def decrypt_source_url(token: str, ingestion_id: str, key: str) -> SignedArchiveSource:
    try:
        payload = json.loads(_cipher(key).decrypt(token.encode("ascii")))
        if payload["version"] != 1 or payload["ingestion_id"] != ingestion_id:
            raise ValueError
        source = parse_archive_source(payload["url"])
        if not isinstance(source, SignedArchiveSource):
            raise ValueError
        return source
    except (InvalidToken, ValueError, KeyError, TypeError, AttributeError):
        raise InvalidArchiveSource(
            "Signed URL credentials are invalid for this ingestion"
        ) from None
