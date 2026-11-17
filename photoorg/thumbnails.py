"""Miniature per la finestra di revisione.

- JPEG/PNG: Pillow (con draft mode per caricare velocemente le foto grandi)
- HEIC/HEIF: pillow-heif, se installato
- RAW: anteprima JPEG incorporata, estratta con exiftool
- Video: un fotogramma con ffmpeg se presente, altrimenti l'eventuale miniatura incorporata
"""
import io
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from .config import RAW_EXT, VIDEO_EXT
from .exif import extract_binary_tag, read_orientation

PILLOW_ERROR = ""
try:
    from PIL import Image, ImageOps
except ImportError as e:  # le anteprime diventano segnaposto testuali
    Image = None
    ImageOps = None
    PILLOW_ERROR = str(e)

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    HEIF_SUPPORT = True
except Exception:
    HEIF_SUPPORT = False

FFMPEG = shutil.which("ffmpeg")
EMBEDDED_TAGS = ("PreviewImage", "JpgFromRaw", "ThumbnailImage")

# Orientamento EXIF -> trasformazione Pillow
_ORIENT_OPS = {2: "FLIP_LEFT_RIGHT", 3: "ROTATE_180", 4: "FLIP_TOP_BOTTOM",
               5: "TRANSPOSE", 6: "ROTATE_270", 7: "TRANSVERSE", 8: "ROTATE_90"}


def pillow_available() -> bool:
    return Image is not None


def make_thumbnail(path: Path, size: int, exiftool: Optional[str]):
    """Ritorna una PIL.Image RGB di lato massimo `size`, oppure None se non si riesce."""
    if Image is None:
        return None
    ext = path.suffix.lower()
    if ext in VIDEO_EXT:
        img = _video_frame(path, exiftool)
    elif ext in RAW_EXT:
        img = _embedded_preview(path, exiftool, rotate=True) or _open_image(path, size)
    else:
        img = _open_image(path, size)
    if img is None:
        return None
    img = img.convert("RGB")
    img.thumbnail((size, size))
    return img


def placeholder_text(path: Path) -> str:
    ext = path.suffix.lower()
    if Image is None:
        return "Anteprima non disponibile\n(installa Pillow:\npip install pillow)"
    if ext in VIDEO_EXT:
        hint = "" if FFMPEG else "\n\n(installa ffmpeg per\nvedere un fotogramma)"
        return f"🎬 VIDEO {ext}{hint}"
    if ext in {".heic", ".heif"} and not HEIF_SUPPORT:
        return "HEIC\n(installa pillow-heif:\npip install pillow-heif)"
    return f"Anteprima non disponibile\n{ext}"


def _open_image(path: Path, size: int):
    try:
        img = Image.open(path)
        try:
            img.draft("RGB", (size * 2, size * 2))  # solo JPEG: decodifica a risoluzione ridotta
        except Exception:
            pass
        return ImageOps.exif_transpose(img)
    except Exception:
        return None


def _bytes_to_image(data: Optional[bytes]):
    if not data:
        return None
    try:
        img = Image.open(io.BytesIO(data))
        img.load()
        return img
    except Exception:
        return None


def _embedded_preview(path: Path, exiftool: Optional[str], rotate: bool):
    if not exiftool:
        return None
    for tag in EMBEDDED_TAGS:
        img = _bytes_to_image(extract_binary_tag(exiftool, path, tag))
        if img is not None:
            if rotate:
                op = _ORIENT_OPS.get(read_orientation(exiftool, path) or 1)
                if op:
                    img = img.transpose(getattr(Image.Transpose, op))
            return img
    return None


def _video_frame(path: Path, exiftool: Optional[str]):
    if FFMPEG:
        for seek in ("1", "0"):  # video più corti di 1s: riprova dall'inizio
            try:
                r = subprocess.run([FFMPEG, "-v", "error", "-ss", seek, "-i", str(path), "-frames:v", "1",
                                    "-f", "image2pipe", "-vcodec", "png", "-"],
                                   capture_output=True, timeout=30)
                img = _bytes_to_image(r.stdout if r.returncode == 0 else None)
                if img is not None:
                    return img
            except Exception:
                break
    return _embedded_preview(path, exiftool, rotate=False)
