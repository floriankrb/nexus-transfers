"""Tests for direct local <-> S3 copies (copy-to-s3 / copy-from-s3)."""

import obstore as obs
import pytest
from obstore.store import MemoryStore

from nexus_transfers import s3 as _s3
from nexus_transfers.copy_s3 import copy_from_s3, copy_to_s3


@pytest.fixture()
def shared_store(monkeypatch):
    """Patch ``s3._store_factory`` to return a single shared in-memory store."""
    store = MemoryStore()
    monkeypatch.setattr(_s3, "_store_factory", lambda: store)
    return store


def _keys(store) -> dict[str, int]:
    return {
        meta["path"]: meta["size"]
        for batch in store.list()
        for meta in batch
    }


def _make_tree(root):
    root.mkdir()
    (root / "a.txt").write_text("hello")
    (root / "sub").mkdir()
    (root / "sub" / "b.bin").write_bytes(bytes(range(256)) * 10)
    return root


@pytest.mark.asyncio
async def test_round_trip_directory(tmp_path, shared_store):
    src = _make_tree(tmp_path / "src")

    await copy_to_s3(str(src), "s3://bucket/pre", quiet=True)
    assert _keys(shared_store) == {
        "pre/a.txt": 5,
        "pre/sub/b.bin": 2560,
    }

    dest = tmp_path / "dest"
    await copy_from_s3("s3://bucket/pre", str(dest), quiet=True)
    assert (dest / "a.txt").read_text() == "hello"
    assert (dest / "sub" / "b.bin").read_bytes() == bytes(range(256)) * 10


@pytest.mark.asyncio
async def test_upload_resume_skips_matching_sizes(tmp_path, shared_store):
    src = _make_tree(tmp_path / "src")
    await copy_to_s3(str(src), "s3://bucket/pre", quiet=True)

    # Corrupt one object so only it is re-uploaded.
    obs.put(shared_store, "pre/a.txt", b"x")
    uploads: list[str] = []
    orig = _s3.upload_file

    def _spy(local_path, *a, **kw):
        uploads.append(kw.get("s3_key"))
        return orig(local_path, *a, **kw)

    import nexus_transfers.s3 as s3_mod
    old = s3_mod.upload_file
    s3_mod.upload_file = _spy
    try:
        await copy_to_s3(str(src), "s3://bucket/pre", quiet=True)
    finally:
        s3_mod.upload_file = old

    assert uploads == ["pre/a.txt"]
    assert _keys(shared_store)["pre/a.txt"] == 5


@pytest.mark.asyncio
async def test_download_resume_skips_matching_sizes(tmp_path, shared_store):
    src = _make_tree(tmp_path / "src")
    await copy_to_s3(str(src), "s3://bucket/pre", quiet=True)

    dest = tmp_path / "dest"
    await copy_from_s3("s3://bucket/pre", str(dest), quiet=True)

    # Truncate one file; only it should be re-fetched.
    (dest / "a.txt").write_text("h")
    (dest / "sub" / "b.bin").write_bytes(b"\0" * 2560)  # same size: skipped
    await copy_from_s3("s3://bucket/pre", str(dest), quiet=True)
    assert (dest / "a.txt").read_text() == "hello"
    # Same-size local file was trusted and not overwritten.
    assert (dest / "sub" / "b.bin").read_bytes() == b"\0" * 2560


@pytest.mark.asyncio
async def test_single_file_upload_and_download(tmp_path, shared_store):
    f = tmp_path / "blob.bin"
    f.write_bytes(b"payload")

    # Explicit key.
    await copy_to_s3(str(f), "s3://bucket/dir/renamed.bin", quiet=True)
    # Trailing slash appends the basename.
    await copy_to_s3(str(f), "s3://bucket/dir2/", quiet=True)
    assert set(_keys(shared_store)) == {"dir/renamed.bin", "dir2/blob.bin"}

    out_file = tmp_path / "out.bin"
    await copy_from_s3("s3://bucket/dir/renamed.bin", str(out_file), quiet=True)
    assert out_file.read_bytes() == b"payload"

    out_dir = tmp_path / "outdir"
    out_dir.mkdir()
    await copy_from_s3("s3://bucket/dir/renamed.bin", str(out_dir), quiet=True)
    assert (out_dir / "renamed.bin").read_bytes() == b"payload"


@pytest.mark.asyncio
async def test_missing_source_raises(tmp_path, shared_store):
    with pytest.raises(FileNotFoundError):
        await copy_to_s3(str(tmp_path / "nope"), "s3://bucket/pre", quiet=True)
    with pytest.raises(FileNotFoundError):
        await copy_from_s3("s3://bucket/nope", str(tmp_path / "d"), quiet=True)
