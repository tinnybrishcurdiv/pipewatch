"""Tests for pipewatch.checkpoint."""
import json
import os
import pytest

from pipewatch.checkpoint import CheckpointDiff, CheckpointEntry, CheckpointStore


# ---------------------------------------------------------------------------
# CheckpointEntry
# ---------------------------------------------------------------------------

class TestCheckpointEntry:
    def test_to_dict_contains_all_keys(self):
        entry = CheckpointEntry(pipeline="etl", offset=42, recorded_at="2024-01-01T00:00:00Z")
        d = entry.to_dict()
        assert d["pipeline"] == "etl"
        assert d["offset"] == 42
        assert d["recorded_at"] == "2024-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# CheckpointDiff
# ---------------------------------------------------------------------------

class TestCheckpointDiff:
    def test_delta_computed_correctly(self):
        diff = CheckpointDiff(pipeline="p", previous=100, current=150)
        assert diff.delta == 50

    def test_delta_none_when_no_previous(self):
        diff = CheckpointDiff(pipeline="p", previous=None, current=10)
        assert diff.delta is None

    def test_str_contains_pipeline(self):
        diff = CheckpointDiff(pipeline="my_pipe", previous=5, current=15)
        assert "my_pipe" in str(diff)

    def test_str_shows_new_when_no_previous(self):
        diff = CheckpointDiff(pipeline="p", previous=None, current=7)
        assert "new" in str(diff)

    def test_str_shows_delta(self):
        diff = CheckpointDiff(pipeline="p", previous=10, current=30)
        assert "delta=+20" in str(diff)

    def test_negative_delta_shown(self):
        diff = CheckpointDiff(pipeline="p", previous=50, current=40)
        assert diff.delta == -10


# ---------------------------------------------------------------------------
# CheckpointStore
# ---------------------------------------------------------------------------

@pytest.fixture()
def store_path(tmp_path):
    return str(tmp_path / "checkpoints.json")


class TestCheckpointStore:
    def test_get_returns_none_for_unknown(self, store_path):
        store = CheckpointStore(path=store_path)
        assert store.get("missing") is None

    def test_update_returns_diff(self, store_path):
        store = CheckpointStore(path=store_path)
        diff = store.update("etl", 100)
        assert isinstance(diff, CheckpointDiff)
        assert diff.current == 100
        assert diff.previous is None

    def test_second_update_has_previous(self, store_path):
        store = CheckpointStore(path=store_path)
        store.update("etl", 100)
        diff = store.update("etl", 200)
        assert diff.previous == 100
        assert diff.delta == 100

    def test_save_writes_valid_json(self, store_path):
        store = CheckpointStore(path=store_path)
        store.update("etl", 55)
        store.save(recorded_at="2024-06-01T12:00:00Z")
        assert os.path.exists(store_path)
        with open(store_path) as fh:
            data = json.load(fh)
        assert len(data) == 1
        assert data[0]["pipeline"] == "etl"
        assert data[0]["offset"] == 55

    def test_load_existing_file(self, store_path):
        # pre-populate file
        entries = [{"pipeline": "pipe_a", "offset": 77, "recorded_at": "2024-01-01T00:00:00Z"}]
        with open(store_path, "w") as fh:
            json.dump(entries, fh)
        store = CheckpointStore(path=store_path)
        assert store.get("pipe_a") == 77

    def test_all_pipelines_sorted(self, store_path):
        store = CheckpointStore(path=store_path)
        store.update("z_pipe", 1)
        store.update("a_pipe", 2)
        assert store.all_pipelines() == ["a_pipe", "z_pipe"]
