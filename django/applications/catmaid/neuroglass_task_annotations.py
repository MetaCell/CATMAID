"""NeuroGlass task associations stay with the original skeleton during a split."""

import re

from catmaid.source_fragments import is_source_fragment_annotation_name


_PUBLIC_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_TASK_ANNOTATION = re.compile(
    rf"neuroglass:study:{_PUBLIC_ID}:task:{_PUBLIC_ID}", re.IGNORECASE)


_COUNTING_PATTERN = re.compile(r"\{n(\d+)\}")


def _creates_task_annotation(name):
    if is_source_fragment_annotation_name(name):
        return False
    # Split annotates one neuron per call: counters use their starting value.
    name = name.replace("{n}", "{n1}")
    while match := _COUNTING_PATTERN.search(name):
        name = name[:match.start()] + str(int(match[1])) + name[match.end():]
    return _TASK_ANNOTATION.fullmatch(name) is not None


def prepare_split_annotations(current, upstream, downstream):
    """Pin existing task links upstream and ignore task links supplied by the caller."""
    tasks = {name: author for name, author in current.items()
            if _TASK_ANNOTATION.fullmatch(name)}
    upstream = {name: author for name, author in upstream.items()
            if not _creates_task_annotation(name)}
    downstream = {name: author for name, author in downstream.items()
            if not _creates_task_annotation(name)}
    return {**upstream, **tasks}, downstream, set(current).difference(tasks)
