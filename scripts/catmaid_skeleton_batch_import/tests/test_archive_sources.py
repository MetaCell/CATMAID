"""NGLASS-2330: signed object URLs remain exact and their credentials stay private."""

from unittest import TestCase
from unittest.mock import patch


from catmaid_skeleton_batch_import import archive_sources as sources

TEST_KEY = "isolated-source-encryption-key-for-tests"
INGESTION_ID = "0198f4af-0b6e-7f57-b55f-644215c4e6c2"
AWS_URL = (
    "https://test-bucket.s3.eu-west-1.amazonaws.com/archive%20name.zip?"
    "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=fixture%2Fscope&"
    "X-Amz-Date=20261016T100000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&"
    "X-Amz-Signature=private-signature&X-Amz-Security-Token=private%2Btoken%2F%3D"
)
GCS_URL = (
    "https://storage.googleapis.com/test-bucket/archive%2Bname.zip?"
    "X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=fixture%40example.com%2Fscope&"
    "X-Goog-Date=20261016T100000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&"
    "X-Goog-Signature=private-signature"
)


class ArchiveSourceTests(TestCase):


    def test_recognizes_official_endpoints_without_rewriting_signed_bytes(self):
        """V2/V4 signatures, temporary tokens and path/virtual hosts must survive verbatim."""
        urls = [
            AWS_URL,
            GCS_URL,
            AWS_URL.replace(
                "test-bucket.s3.eu-west-1.amazonaws.com/",
                "s3.eu-west-1.amazonaws.com/test-bucket/",
            ),
            AWS_URL.replace("s3.eu-west-1", "s3.dualstack.eu-west-1"),
            GCS_URL.replace(
                "storage.googleapis.com/test-bucket/",
                "test-bucket.storage.googleapis.com/",
            ),
            "https://s3.amazonaws.com/test-bucket/file.zip?AWSAccessKeyId=fixture&Expires=123&Signature=a%2Bb%2F%3D",
            "https://storage.googleapis.com/test-bucket/file.zip?GoogleAccessId=fixture&Expires=123&Signature=a%2Bb%2F%3D",
        ]
        for url in urls:
            with self.subTest(url=url.split("?", 1)[0]):
                source = sources.parse_archive_source(url)
                self.assertEqual(
                    source.url,
                    url,
                    "Signing-sensitive URL bytes must not be normalized",
                )
                self.assertNotIn(
                    "?",
                    source.uri,
                    "Stored locations must exclude all query parameters",
                )
                self.assertNotIn(
                    "Signature",
                    repr(source),
                    "Object representations must conceal credentials",
                )
                self.assertNotIn(
                    "private-signature",
                    str(source.metadata()),
                    "Durable metadata must be safe to publish",
                )

    def test_rejects_unsafe_or_unsupported_sources_without_echoing_them(self):
        """Host allowlisting and strict URL syntax prevent arbitrary server-side fetches."""
        invalid = [
            AWS_URL.replace("https:", "http:"),
            AWS_URL.replace("test-bucket.s3.eu-west-1.amazonaws.com", "127.0.0.1"),
            AWS_URL.replace(
                "test-bucket.s3.eu-west-1.amazonaws.com", "metadata.google.internal"
            ),
            AWS_URL.replace("amazonaws.com", "amazonaws.com.evil.example"),
            GCS_URL.replace("storage.googleapis.com", "storage.cloud.google.com"),
            GCS_URL.replace("storage.googleapis.com", "storage.googleapis.com:444"),
            GCS_URL.replace(
                "storage.googleapis.com", "user:password@storage.googleapis.com"
            ),
            GCS_URL + "#fragment",
            GCS_URL + "\n",
            GCS_URL + "&X-Goog-Signature=duplicate",
            GCS_URL.replace("SignedHeaders=host", "SignedHeaders=host%3Bx-goog-extra"),
            GCS_URL.split("?", 1)[0],
        ]
        for url in invalid:
            with self.subTest(location=url.split("?", 1)[0]):
                with self.assertRaises(sources.InvalidArchiveSource) as raised:
                    sources.parse_archive_source(url)
                self.assertNotIn(
                    "private-signature",
                    str(raised.exception),
                    "Validation errors must never echo bearer tokens",
                )

    def test_authenticated_encryption_binds_credentials_to_the_ingestion(self):
        """Workflow ciphertext cannot be moved between ingestion identities or altered."""
        token = sources.encrypt_source_url(AWS_URL, INGESTION_ID, TEST_KEY)
        self.assertEqual(
            sources.decrypt_source_url(token, INGESTION_ID, TEST_KEY).url, AWS_URL
        )
        self.assertNotIn(
            "private-signature",
            token,
            "Ciphertext must not contain plaintext credentials",
        )
        for bad_token, ingestion_id, key in [
            (token, "different-ingestion", TEST_KEY),
            (token, INGESTION_ID, "different-source-encryption-key"),
            (token[:-4] + "xxxx", INGESTION_ID, TEST_KEY),
        ]:
            with self.assertRaises(sources.InvalidArchiveSource):
                sources.decrypt_source_url(bad_token, ingestion_id, key)
