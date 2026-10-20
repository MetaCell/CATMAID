"""Normalized filename provenance shared with CATMAID through a tested contract."""

from pathlib import PurePosixPath

from .errors import InvalidInputError


SOURCE_FRAGMENT_ANNOTATION_PREFIX = "source-fragment:"
MAX_FRAGMENT_ID_LENGTH = 255 - len(SOURCE_FRAGMENT_ANNOTATION_PREFIX)
CONCEPT_IDS_PER_MEMBER = 5


def fragment_id_from_member_path(member_path: str) -> str:
    """Remove the basename's final SWC extension and surrounding whitespace."""
    filename = PurePosixPath(member_path).name
    if not filename.lower().endswith(".swc"):
        raise InvalidInputError(f"SWC member {member_path!r} has no .swc extension")
    fragment_id = filename[:-4].strip()
    if not fragment_id:
        raise InvalidInputError(f"SWC member {member_path!r} has an empty fragment identifier")
    if len(fragment_id) > MAX_FRAGMENT_ID_LENGTH:
        raise InvalidInputError(
            f"SWC member {member_path!r} fragment identifier exceeds "
            f"{MAX_FRAGMENT_ID_LENGTH} characters"
        )
    if "\x00" in fragment_id:
        raise InvalidInputError(f"SWC member {member_path!r} has a NUL in its fragment identifier")
    return fragment_id


def fragment_annotation_name(fragment_id: str) -> str:
    if not isinstance(fragment_id, str) or not fragment_id:
        raise ValueError("fragment identifier must be a non-empty string")
    if fragment_id != fragment_id.strip():
        raise ValueError("fragment identifier must not have surrounding whitespace")
    if len(fragment_id) > MAX_FRAGMENT_ID_LENGTH or "\x00" in fragment_id:
        raise ValueError("fragment identifier cannot be represented as a CATMAID annotation")
    return SOURCE_FRAGMENT_ANNOTATION_PREFIX + fragment_id
