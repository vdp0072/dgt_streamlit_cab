"""
Compatibility shim for imghdr module (packaged for pip installation).
This is identical to the top-level imghdr.py shim.
"""
from __future__ import annotations

import typing

try:
    from PIL import Image
except Exception:
    Image = None  # type: ignore


def _read_head(file) -> bytes:
    if file is None:
        return b""
    if isinstance(file, (bytes, bytearray)):
        return bytes(file[:32])
    try:
        with open(file, "rb") as f:
            return f.read(32)
    except Exception:
        try:
            pos = file.tell()
            head = file.read(32)
            file.seek(pos)
            return head
        except Exception:
            return b""


def what(file, h: typing.Optional[bytes] = None) -> typing.Optional[str]:
    if Image is not None:
        try:
            if h is None:
                if isinstance(file, (bytes, bytearray)):
                    im = Image.open(_BytesIO(file))
                else:
                    im = Image.open(file)
            else:
                im = Image.open(_BytesIO(h))
            fmt = im.format
            if fmt:
                return fmt.lower()
        except Exception:
            pass

    if h is None:
        h = _read_head(file)
    if not h:
        return None

    if h.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if h.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if h[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if h.startswith(b"BM"):
        return "bmp"
    if h.startswith(b"II*") or h.startswith(b"MM\x00*"):
        return "tiff"
    if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
        return "webp"

    return None


class _BytesIO:
    def __init__(self, b: bytes):
        self._b = b
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        if n < 0:
            n = len(self._b) - self._pos
        data = self._b[self._pos : self._pos + n]
        self._pos += len(data)
        return data

    def seek(self, pos: int, whence: int = 0):
        if whence == 0:
            self._pos = pos
        elif whence == 1:
            self._pos += pos
        elif whence == 2:
            self._pos = len(self._b) + pos

    def tell(self) -> int:
        return self._pos
