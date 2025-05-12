from hashlib import sha256


def hash_buffer(buffer: bytes) -> str:
    return sha256(buffer).hexdigest()
