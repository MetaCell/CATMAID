"""NeuroGlass task associations stay with the original skeleton during a split."""

import re

from catmaid.neuroglass_tasks import TASK_ANNOTATION_NAMESPACE, is_task_annotation


_COUNTING_PATTERN = re.compile(r"\{n(\d+)\}")


def _creates_task_annotation(name):
    if not TASK_ANNOTATION_NAMESPACE.match(name):
        return False
    # Split annotates one neuron per call: counters use their starting value.
    name = name.replace("{n}", "{n1}")
    while match := _COUNTING_PATTERN.search(name):
        name = name[:match.start()] + str(int(match[1])) + name[match.end():]
    return is_task_annotation(name)


def prepare_split_annotations(current, upstream, downstream):
    """Pin existing task links upstream and ignore task links supplied by the caller."""
    tasks = {name: author for name, author in current.items()
            if is_task_annotation(name)}
    upstream = {name: author for name, author in upstream.items()
            if not _creates_task_annotation(name)}
    downstream = {name: author for name, author in downstream.items()
            if not _creates_task_annotation(name)}
    return {**upstream, **tasks}, downstream, set(current).difference(tasks)
