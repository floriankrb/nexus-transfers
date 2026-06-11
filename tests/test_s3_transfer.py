"""Integration tests for S3-staged file transfer."""

import os

import obstore as obs
import pytest
from obstore.store import MemoryStore

from nexus_transfers import Client, s3 as _s3
from nexus_transfers.client import RemoteError


@pytest.fixture()
def shared_store(monkeypatch):
    """Patch ``s3.get_store`` to return a single shared in-memory store."""
    store = MemoryStore()
    monkeypatch.setattr(_s3, "_store_factory", lambda: store)
    return store


@pytest.mark.asyncio
async def test_s3_file_transfer(broker, tmp_path, shared_store):
    src_file = tmp_path / "data.bin"
    content = bytes(range(256)) * 50
    src_file.write_bytes(content)

    async with (
        Client("sender", url=broker, allowed_paths=[str(tmp_path)]) as sender,
        Client("receiver", url=broker) as receiver,
    ):
        result = await receiver.send("sender.get_file", str(src_file))
        # S3 path returns a temp file path; read and verify contents.
        assert isinstance(result, str)
        with open(result, "rb") as fh:
            data = fh.read()
        os.unlink(result)
        assert data == content

        # Cleanup propagates: drain the cleanup task and check the bucket.
        for _ in range(20):
            import asyncio
            await asyncio.sleep(0.05)
            if not sender._s3_keys:
                break
        assert sender._s3_keys == set()
        # The shared store should be empty.
        listing = list(shared_store.list().collect())
        assert listing == []


@pytest.mark.asyncio
async def test_s3_directory_transfer(broker, tmp_path, shared_store):
    src = tmp_path / "remote"
    src.mkdir()
    (src / "a.txt").write_text("hello")
    (src / "sub").mkdir()
    (src / "sub" / "b.txt").write_text("world")

    dest = tmp_path / "local"

    async with (
        Client("srv", url=broker, allowed_paths=[str(src)]) as srv,
        Client("cli", url=broker) as cli,
    ):
        await cli.get_directory("srv", str(src), str(dest))

    assert (dest / "a.txt").read_text() == "hello"
    assert (dest / "sub" / "b.txt").read_text() == "world"


@pytest.mark.asyncio
async def test_s3_cleanup_rejects_unknown_key(broker, tmp_path, shared_store):
    async with (
        Client("a", url=broker, allowed_paths=[str(tmp_path)]) as a,
        Client("b", url=broker) as b,
    ):
        with pytest.raises(RemoteError, match="unknown S3 key"):
            await b.send("a.s3_cleanup", "nexus-transfers/fake/key")


def test_upload_download_round_trip(tmp_path, shared_store):
    """Direct upload/download without the WebSocket layer."""
    src = tmp_path / "blob.bin"
    src.write_bytes(os.urandom(50_000))

    bucket, s3_key, size, checksum = _s3.upload_file(str(src))
    assert size == src.stat().st_size

    data = _s3.download_bytes(s3_key, target_path=str(tmp_path / "downloaded.bin"), expected_checksum=checksum, bucket=bucket)
    assert data == src.read_bytes()

    _s3.delete(s3_key)


def test_normalise_bucket_strips_scheme():
    assert _s3._normalise_bucket("my-bucket") == "my-bucket"
    assert _s3._normalise_bucket("s3://my-bucket") == "my-bucket"
    assert _s3._normalise_bucket("s3://my-bucket/") == "my-bucket"
    assert _s3._normalise_bucket("s3://my-bucket///") == "my-bucket"


def test_make_key_uses_source_path():
    assert _s3.make_key("/tmp/foo.bin") == "tmp/foo.bin"
    assert _s3.make_key("/a/b/c.txt") == "a/b/c.txt"


def test_make_key_with_s3_prefix():
    prefix = "2025-05-04-143025-src-dst-abcd1234"
    assert _s3.make_key("/tmp/foo.bin", s3_prefix=prefix) == f"{prefix}/tmp/foo.bin"
    assert _s3.make_key("/a/b/c.txt", s3_prefix=prefix) == f"{prefix}/a/b/c.txt"


def test_split_bucket_spec_extracts_prefix():
    assert _s3._split_bucket_spec("my-bucket") == ("my-bucket", None)
    assert _s3._split_bucket_spec("s3://my-bucket") == ("my-bucket", None)
    assert _s3._split_bucket_spec("s3://my-bucket/staging") == ("my-bucket", "staging")
    assert _s3._split_bucket_spec("s3://my-bucket/staging/") == ("my-bucket", "staging")
    assert _s3._split_bucket_spec("s3://my-bucket/a/b/c") == ("my-bucket", "a/b/c")
    assert _s3._split_bucket_spec("my-bucket/sub") == ("my-bucket", "sub")


def test_download_checksum_mismatch_raises(tmp_path, shared_store):
    src = tmp_path / "blob.bin"
    src.write_bytes(b"abc")
    bucket, s3_key, _, _ = _s3.upload_file(str(src))
    with pytest.raises(ValueError, match="checksum mismatch"):
        _s3.download_bytes(s3_key, target_path=str(tmp_path / "bad.bin"), expected_checksum="0" * 64, bucket=bucket)
