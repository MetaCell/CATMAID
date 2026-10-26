from unittest import TestCase
from catmaid_skeleton_batch_import.workflow_progress import progress_units, FINAL_PHASES

class NativeProgressUnitsTests(TestCase):
    def state(self, statuses, **phases):
        return {
            "schema_version": 1,
            "run": {"status": "running"},
            "plan": {"batch_count": len(statuses)},
            "batches": [{"status": status} for status in statuses],
            "phases": {
                name: {"status": phases.get(name, "pending")} for name in FINAL_PHASES
            },
        }

    def test_counts_confirmed_commits_then_completed_final_phases(self):
        for statuses, expected in (
            (["planned", "planned"], (3, 15)),
            (["committed", "prepared"], (4, 15)),
            (["committed", "failed"], (4, 15)),
            (["committed", "committed"], (2, 5)),
        ):
            self.assertEqual(progress_units(self.state(statuses)), expected)
        completed = {}
        for i, phase in enumerate(FINAL_PHASES, start=3):
            completed[phase] = "complete"
            self.assertEqual(
                progress_units(self.state(["committed"] * 2, **completed)), (i, 5)
            )

    def test_invalid_state_is_not_reported(self):
        state = self.state(["committed", "planned"])
        for invalid in (
            None,
            {},
            *({**state, "schema_version": version} for version in (0, 2, 3, 4, 99)),
            {**state, "plan": {"batch_count": True}},
            {**state, "plan": {"batch_count": 3}},
            {**state, "batches": [{"status": []}, {"status": "planned"}]},
        ):
            self.assertIsNone(progress_units(invalid))



class CacheProgressUnitsTests(TestCase):
    def test_runner_counts_cells_only_during_running_cache_phase(self):
        state = NativeProgressUnitsTests().state(["committed"]*2, database_maintenance="complete", cache="running")
        units = progress_units(state, {"schema_version":1,"completed":800,"total":1000})
        self.assertEqual(units, (3803, 5005))
        for invalid in ({"schema_version":1,"completed":True,"total":2},
                        {"schema_version":1,"completed":3,"total":2}, {}, None):
            self.assertEqual(progress_units(state, invalid), (3,5))
