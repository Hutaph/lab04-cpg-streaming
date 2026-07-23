import hashlib


def compute_generation_id(file_id: str, content_hash: str, parser_version: str, schema_version: str) -> str:
    """Computes a unique, deterministic generation fingerprint for nodes/edges/placeholders.

    Uses SHA256 of the concatenated string formatted as:
    file_id:content_hash:parser_version:schema_version
    encoded in UTF-8.
    """
    separator = ":"
    raw_str = f"{file_id}{separator}{content_hash}{separator}{parser_version}{separator}{schema_version}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()
