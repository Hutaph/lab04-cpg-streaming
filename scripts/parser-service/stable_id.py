import hashlib


def stable_id(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_sha256(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()
