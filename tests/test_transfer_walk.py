"""Unit tests for _DirectoryTransfer's remote walk (mocked client, no broker)."""

import asyncio

from nexus_transfers.client._transfer import _DirectoryTransfer


class _FakeProgress:
    def add_task(self, *args, **kwargs):
        return 1

    def update(self, *args, **kwargs):
        pass

    def remove_task(self, *args, **kwargs):
        pass


class _FakeClient:
    """Serves paginated list_dir results from an in-memory tree."""

    _progress = _FakeProgress()
    peer_delay = 0.0
    name = "fake"

    def __init__(self, tree):
        self._tree = tree

    async def send(self, target_func, path, **kwargs):
        offset, limit = kwargs["offset"], kwargs["limit"]
        return self._tree[path][offset:offset + limit]

    async def monitor(self, *args, **kwargs):
        pass


async def _walk(tree, root, tmp_path):
    xfer = _DirectoryTransfer(
        client=_FakeClient(tree), target="t", remote_path=root,
        local_path=str(tmp_path / "out"), max_concurrent=4,
        chunk_size=65536, use_s3=False, s3_prefix=None, track_bytes=False,
    )
    queue: asyncio.Queue = asyncio.Queue()
    await xfer._walk_remote(root, str(tmp_path / "out"), queue)
    files = []
    while not queue.empty():
        files.append(queue.get_nowait()[0])
    return files


async def test_walk_remote_recurses_dirs_from_all_pages(tmp_path):
    """Regression (BUG 3): subdirectories listed in non-final pages were lost."""
    tree = {
        "root": sorted(
            [{"name": f"f{i:04d}", "type": "file"} for i in range(1499)]
            + [{"name": "aaa_dir", "type": "dir"}],
            key=lambda e: e["name"],
        ),
        "root/aaa_dir": [{"name": "inner", "type": "file"}],
    }
    files = await _walk(tree, "root", tmp_path)
    assert len(files) == 1500
    assert "root/aaa_dir/inner" in files


async def test_walk_remote_exact_page_multiple(tmp_path):
    """Regression (BUG 3): an entry count that is an exact multiple of the
    page size made the final empty page wipe the directory list entirely."""
    tree = {
        "root": sorted(
            [{"name": f"g{i:04d}", "type": "file"} for i in range(999)]
            + [{"name": "bbb_dir", "type": "dir"}],
            key=lambda e: e["name"],
        ),
        "root/bbb_dir": [{"name": "deep", "type": "file"}],
    }
    files = await _walk(tree, "root", tmp_path)
    assert len(files) == 1000
    assert "root/bbb_dir/deep" in files


async def test_walk_remote_small_dir(tmp_path):
    tree = {
        "root": [
            {"name": "a", "type": "file"},
            {"name": "sub", "type": "dir"},
        ],
        "root/sub": [{"name": "b", "type": "file"}],
    }
    files = await _walk(tree, "root", tmp_path)
    assert sorted(files) == ["root/a", "root/sub/b"]
