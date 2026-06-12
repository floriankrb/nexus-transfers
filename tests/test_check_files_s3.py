"""Tests for verifying an S3 copy against the local reference."""

import obstore as obs
import pytest
from obstore.store import MemoryStore

from nexus_transfers import s3 as _s3
from nexus_transfers.check_files_s3 import _check_s3
from nexus_transfers.copy_s3 import copy_to_s3


@pytest.fixture()
def shared_store(monkeypatch):
    """Patch ``s3._store_factory`` to return a single shared in-memory store."""
    store = MemoryStore()
    monkeypatch.setattr(_s3, "_store_factory", lambda: store)
    return store


@pytest.fixture()
def tree(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("hello")
    (src / "sub").mkdir()
    (src / "sub" / "b.bin").write_bytes(bytes(range(256)))
    return src


async def _check(src, **kw):
    return await _check_s3(
        str(src), "s3://bucket/pre", None, "tester", None, quiet=True, **kw,
    )


@pytest.mark.asyncio
async def test_clean_copy_passes(tree, shared_store):
    await copy_to_s3(str(tree), "s3://bucket/pre", quiet=True)
    report = await _check(tree)
    assert report.ok
    assert report.checked == 2


@pytest.mark.asyncio
async def test_size_mismatch_detected_and_fixed(tree, shared_store):
    await copy_to_s3(str(tree), "s3://bucket/pre", quiet=True)
    obs.put(shared_store, "pre/a.txt", b"bad-length")

    report = await _check(tree)
    assert not report.ok
    assert [rel for rel, _, _ in report.discrepancies["corrupt"]] == ["a.txt"]

    report = await _check(tree, fix=True)
    assert report.ok
    assert obs.get(shared_store, "pre/a.txt").bytes().to_bytes() == b"hello"


@pytest.mark.asyncio
async def test_hash_catches_same_size_corruption(tree, shared_store):
    await copy_to_s3(str(tree), "s3://bucket/pre", quiet=True)
    obs.put(shared_store, "pre/a.txt", b"hellx")  # same size, wrong bytes

    report = await _check(tree)
    assert report.ok  # size-only check cannot see it

    report = await _check(tree, algo="md5")
    assert not report.ok
    assert [rel for rel, _, _ in report.discrepancies["corrupt"]] == ["a.txt"]


@pytest.mark.asyncio
async def test_missing_and_extra(tree, shared_store):
    await copy_to_s3(str(tree), "s3://bucket/pre", quiet=True)
    obs.delete(shared_store, "pre/sub/b.bin")
    obs.put(shared_store, "pre/stray.txt", b"stray")

    report = await _check(tree)
    assert not report.ok
    assert [rel for rel, _, _ in report.discrepancies["missing"]] == ["sub/b.bin"]
    assert [rel for rel, _, _ in report.discrepancies["extra"]] == ["stray.txt"]

    report = await _check(tree, fix=True, delete_extra=True)
    assert report.ok
    keys = {
        meta["path"]
        for batch in shared_store.list()
        for meta in batch
    }
    assert keys == {"pre/a.txt", "pre/sub/b.bin"}
