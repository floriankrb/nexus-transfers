"""Tests for nexus_transfers.dispatch – pure functions, no server needed."""

import os
import tempfile

import pytest

from nexus_transfers.dispatch import (
    adder,
    echo,
    make_get_file,
    make_list_dir,
    resolve_safe_path,
    FileTransfer,
)


class TestAdder:
    def test_increments(self):
        assert adder(0) == 1
        assert adder(41) == 42
        assert adder(-1) == 0

    def test_float(self):
        assert adder(1.5) == 2.5


class TestEcho:
    def test_single_arg(self):
        assert echo("hello") == "hello"

    def test_multiple_args(self):
        assert echo("a", "b") == ["a", "b"]


class TestResolveSafePath:
    def test_allowed_path(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        resolved = resolve_safe_path(str(f), [str(tmp_path)])
        assert resolved == os.path.realpath(str(f))

    def test_reject_dotdot(self, tmp_path):
        with pytest.raises((ValueError, PermissionError)):
            resolve_safe_path(str(tmp_path / ".." / "etc"), [str(tmp_path)])

    def test_reject_outside(self, tmp_path):
        with pytest.raises(PermissionError, match="outside"):
            resolve_safe_path("/etc/passwd", [str(tmp_path)])

    def test_allowed_dir_itself(self, tmp_path):
        resolved = resolve_safe_path(str(tmp_path), [str(tmp_path)])
        assert resolved == os.path.realpath(str(tmp_path))


class TestMakeGetFile:
    def test_returns_file_transfer(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"hello")
        get_file = make_get_file([str(tmp_path)])
        result = get_file(str(f), use_s3=False)
        assert isinstance(result, FileTransfer)
        assert result.size == 5

    def test_rejects_outside(self, tmp_path):
        get_file = make_get_file([str(tmp_path)])
        with pytest.raises(PermissionError):
            get_file("/etc/passwd")


class TestMakeListDir:
    def test_lists_entries(self, tmp_path):
        (tmp_path / "a.txt").write_text("hi")
        (tmp_path / "subdir").mkdir()
        list_dir = make_list_dir([str(tmp_path)])
        entries = list_dir(str(tmp_path))
        names = {e["name"] for e in entries}
        assert "a.txt" in names
        assert "subdir" in names
        file_entry = next(e for e in entries if e["name"] == "a.txt")
        assert file_entry["type"] == "file"
        assert "size" not in file_entry
        dir_entry = next(e for e in entries if e["name"] == "subdir")
        assert dir_entry["type"] == "dir"

    def test_lists_entries_with_size(self, tmp_path):
        (tmp_path / "a.txt").write_text("hi")
        (tmp_path / "subdir").mkdir()
        list_dir = make_list_dir([str(tmp_path)])
        entries = list_dir(str(tmp_path), include_size=True)
        file_entry = next(e for e in entries if e["name"] == "a.txt")
        assert file_entry["size"] == 2
        dir_entry = next(e for e in entries if e["name"] == "subdir")
        assert "size" not in dir_entry

    def test_rejects_outside(self, tmp_path):
        list_dir = make_list_dir([str(tmp_path)])
        with pytest.raises(PermissionError):
            list_dir("/etc")

    def test_rejects_file(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_text("x")
        list_dir = make_list_dir([str(tmp_path)])
        with pytest.raises(NotADirectoryError):
            list_dir(str(f))

    def test_pages_come_from_one_snapshot(self, tmp_path):
        for name in ("b", "c", "d", "e"):
            (tmp_path / name).write_text("x")
        list_dir = make_list_dir([str(tmp_path)])
        page1 = list_dir(str(tmp_path), offset=0, limit=2)
        # A file sorting first would shift every offset if pages rescanned.
        (tmp_path / "a").write_text("x")
        page2 = list_dir(str(tmp_path), offset=2, limit=2)
        names = [e["name"] for e in page1 + page2]
        assert names == ["b", "c", "d", "e"]
        # A new walk (offset 0) takes a fresh snapshot and sees the new file.
        assert [e["name"] for e in list_dir(str(tmp_path), offset=0, limit=2)] == ["a", "b"]

    def test_vanished_entry_does_not_shorten_page(self, tmp_path):
        for name in ("a", "b", "c", "d"):
            (tmp_path / name).write_text("x")
        list_dir = make_list_dir([str(tmp_path)])
        list_dir(str(tmp_path), offset=0, limit=2)
        (tmp_path / "c").unlink()
        # A short page would end the caller's pagination loop early, so the
        # vanished entry must still be reported (as a file, without size).
        page = list_dir(str(tmp_path), offset=2, limit=2, include_size=True)
        assert [e["name"] for e in page] == ["c", "d"]
        vanished = page[0]
        assert vanished["type"] == "file"
        assert "size" not in vanished
