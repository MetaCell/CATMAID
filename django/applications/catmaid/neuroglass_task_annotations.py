"""NeuroGlass task associations stay with the original skeleton during a split."""

import re


_PUBLIC_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_TASK_ANNOTATION = re.compile(
    rf"neuroglass:study:{_PUBLIC_ID}:task:{_PUBLIC_ID}", re.IGNORECASE)


def prepare_split_annotations(current, upstream, downstream):
    """Pin existing task links upstream and ignore task links supplied by the caller."""
    tasks = {name: author for name, author in current.items()
            if _TASK_ANNOTATION.fullmatch(name)}
    upstream = {name: author for name, author in upstream.items()
            if not _TASK_ANNOTATION.fullmatch(name)}
    downstream = {name: author for name, author in downstream.items()
            if not _TASK_ANNOTATION.fullmatch(name)}
    return {**upstream, **tasks}, downstream, set(current).difference(tasks)
