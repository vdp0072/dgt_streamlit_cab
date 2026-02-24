"""
Minimal shim for the stdlib `imghdr` module.

Some Python runtime environments may lack the built-in `imghdr` module
or it might be removed/deprecated. Streamlit imports `imghdr` for image
type detection. This small compatibility shim implements a subset of
`imghdr.what()` sufficient for common image types (jpeg, png, gif,
bmp, tiff, webp) using simple signature checks. If Pillow is available
it will be used for more robust detection.

This file is intentionally lightweight and only implements what's
needed by streamlit in typical cases.
"""
from __future__ import annotations

import typing

try:
    # Prefer Pillow when available for robust detection
    from PIL import Image
except Exception:
    Image = None  # type: ignore


def _read_head(file) -> bytes:
    """Return up to 32 header bytes from file or bytes-like input."""
    if file is None:
        return b""
    # If a bytes-like object was passed
    if isinstance(file, (bytes, bytearray)):
        return bytes(file[:32])
    try:
        # file might be a filename
        with open(file, "rb") as f:
            return f.read(32)
    except Exception:
        # file might be a file-like object
        try:
            pos = file.tell()
            head = file.read(32)
            file.seek(pos)
            return head
        except Exception:
            return b""


def what(file, h: typing.Optional[bytes] = None) -> typing.Optional[str]:
    """Detect image type. Returns a string like 'png','jpeg', or None."""
    # If Pillow is available and file is a path or file-like, prefer it
    if Image is not None:
        try:
            if h is None:
                # If file is bytes-like, load from bytes
                if isinstance(file, (bytes, bytearray)):
                    im = Image.open(_BytesIO(file))
                else:
                    im = Image.open(file)
            else:
                # use header bytes via BytesIO
                im = Image.open(_BytesIO(h))
            fmt = im.format
            if fmt:
                return fmt.lower()
        except Exception:
            # fall back to signature checks
            pass

    if h is None:
        h = _read_head(file)
    if not h:
        return None

    # JPEG
    if h.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    # PNG
    if h.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    # GIF
    if h[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    # BMP
    if h.startswith(b"BM"):
        return "bmp"
    # TIFF (little/big endian)
    if h.startswith(b"II*") or h.startswith(b"MM\x00*"):
        return "tiff"
    # WebP (RIFF....WEBP)
    if h[:4] == b"RIFF" and h[8:12] == b"WEBP":
        return "webp"

    return None


# minimal BytesIO wrapper to avoid importing io unless needed
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
