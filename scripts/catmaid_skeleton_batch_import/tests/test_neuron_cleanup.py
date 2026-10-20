"""Exercise CATMAID deletion cleanup without bootstrapping its web stack."""

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from catmaid_skeleton_batch_import.errors import InvalidInputError
from catmaid_skeleton_batch_import.loader import ProjectContext
from catmaid_skeleton_batch_import.orchestrator import _check_existing_fragment_conflicts
from catmaid_skeleton_batch_import.planner import build_plan
from test_existing_projects import Connection, CONTEXT
from test_planner import member


@pytest.mark.parametrize("shared", [False, True])
def test_deletion_releases_only_unused_annotations_and_keeps_real_collisions(shared):
    annotations = {13: "source-fragment:alpha", 23: "ordinary", 43: "unrelated"}
    links = [
        {"id": 14, "project_id": 123, "class_instance_a_id": 10,
         "class_instance_b_id": 13, "relation__relation_name": "annotated_with"},
        {"id": 24, "project_id": 123, "class_instance_a_id": 10,
         "class_instance_b_id": 23, "relation__relation_name": "annotated_with"},
    ]
    if shared:
        links.append({"id": 34, "project_id": 123, "class_instance_a_id": 30,
                      "class_instance_b_id": 13, "relation__relation_name": "annotated_with"})
    def filter_links(**filters):
        rows = links
        for key, value in filters.items():
            if key.endswith("__in"):
                rows = [row for row in rows if row[key[:-4]] in value]
            else:
                rows = [row for row in rows if row[key] == value]
        return SimpleNamespace(values_list=lambda field, **_kwargs: [row[field] for row in rows])
    def filter_annotations(**filters):
        assert filters["project_id"] == 123
        assert filters["class_column__class_name"] == "annotation"
        def delete():
            for identifier in filters["id__in"]:
                annotations.pop(identifier)
        return SimpleNamespace(delete=delete)
    def delete_neuron():
        links[:] = [row for row in links if row["class_instance_a_id"] != 10]
    path = Path(__file__).resolve().parents[3] / "django/applications/catmaid/control/neuron.py"
    function = next(node for node in ast.parse(path.read_text()).body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "_delete_neuron_and_unused_annotations")
    namespace = {
        "ClassInstanceClassInstance": SimpleNamespace(objects=SimpleNamespace(filter=filter_links)),
        "ClassInstance": SimpleNamespace(objects=SimpleNamespace(filter=filter_annotations)),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)

    namespace[function.name](SimpleNamespace(id=10, project_id=123, delete=delete_neuron))

    assert annotations == ({13: "source-fragment:alpha", 43: "unrelated"}
                           if shared else {43: "unrelated"})
    store = SimpleNamespace(load=lambda: {"batches": [{"index": 1, "status": "planned"}]})
    args = (store, build_plan([member("alpha.swc", 2)], 10),
            ProjectContext.from_dict(CONTEXT), Connection(list(annotations.items())))
    if shared:
        with pytest.raises(InvalidInputError, match="already exist"):
            _check_existing_fragment_conflicts(*args)
    else:
        _check_existing_fragment_conflicts(*args)
