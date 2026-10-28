import json
from unittest.mock import patch

from catmaid_skeleton_batch_import.cache_progress import CacheProgressWriter


def test_observation_file_is_throttled_but_initial_reset_and_final_are_saved(tmp_path):
    writer=CacheProgressWriter(tmp_path, clock=iter([0,0.2,1.1,1.2,1.3]).__next__)
    writer(0,3); writer(1,3)
    assert json.loads(writer.path.read_text())["completed"] == 0
    writer(2,3)
    assert json.loads(writer.path.read_text())["completed"] == 2
    writer(3,3)
    assert json.loads(writer.path.read_text()) == {"schema_version":1,"completed":3,"total":3}
    writer(0,3)
    assert json.loads(writer.path.read_text())["completed"] == 0


def test_observation_write_failure_is_best_effort(tmp_path):
    writer=CacheProgressWriter(tmp_path)
    with patch("catmaid_skeleton_batch_import.cache_progress.atomic_write_json",side_effect=OSError("full")):
        writer(0,10)



def test_failed_observations_are_also_throttled(tmp_path):
    writer=CacheProgressWriter(tmp_path, clock=iter([0,0.1,0.2,1.1,1.2]).__next__)
    with patch("catmaid_skeleton_batch_import.cache_progress.atomic_write_json",side_effect=OSError("full")) as write:
        for completed in (0,1,2,3,10):
            writer(completed,10)
    assert write.call_count == 3
