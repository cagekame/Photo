"""Utilità su file: hash, scansione cartelle, sidecar, log."""
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable, Iterator, Tuple

from .config import ALL_EXT, QUARANTINE_PREFIX, SIDECAR_EXT


def file_sha1(p: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()


def file_sha1_head(p: Path, max_bytes: int) -> str:
    h = hashlib.sha1()
    read = 0
    with p.open("rb") as f:
        while read < max_bytes:
            chunk = f.read(min(1024 * 1024, max_bytes - read))
            if not chunk:
                break
            h.update(chunk)
            read += len(chunk)
    return h.hexdigest()


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"


def append_jsonl(path: Path, obj: dict):
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def ensure_dir(p: Path, dry_run: bool):
    if not dry_run:
        p.mkdir(parents=True, exist_ok=True)


def sidecars_for(p: Path) -> Iterable[Path]:
    """Sidecar con lo stesso nome base, nella stessa cartella del file."""
    stem = p.with_suffix("").name
    for ext in SIDECAR_EXT:
        cand = p.parent / f"{stem}{ext}"
        try:
            if cand.is_file():
                yield cand
        except Exception:
            continue


def other_media_with_stem(sidecar: Path, exclude: set) -> bool:
    """True se nella cartella del sidecar c'è ancora un file media con lo stesso nome base,
    escluso quanto in `exclude` (es. IMG_1.CR2 + IMG_1.JPG condividono IMG_1.xmp)."""
    stem = sidecar.stem.lower()
    try:
        for q in sidecar.parent.iterdir():
            if (_is_media(q.name) and q.stem.lower() == stem
                    and q not in exclude and q.is_file()):
                return True
    except Exception:
        return True  # nel dubbio considera il sidecar condiviso e non toccarlo
    return False


def _is_media(name: str) -> bool:
    return os.path.splitext(name)[1].lower() in ALL_EXT


def walk_files(base: Path, recursive: bool) -> Iterator[Path]:
    """File foto/video sotto base. Le cartelle di quarantena vengono sempre escluse."""
    stack = [base]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            if recursive and not e.name.startswith(QUARANTINE_PREFIX):
                                stack.append(Path(e.path))
                        elif e.is_file(follow_symlinks=False) and _is_media(e.name):
                            yield Path(e.path)
                    except Exception:
                        continue
        except Exception:
            continue


def iter_candidates_fast(folder: Path) -> Iterator[Tuple[float, Path]]:
    """(mtime, path) dei file media nella sola cartella indicata (non ricorsivo)."""
    with os.scandir(folder) as it:
        for entry in it:
            if not entry.is_file(follow_symlinks=False) or not _is_media(entry.name):
                continue
            try:
                mtime = entry.stat(follow_symlinks=False).st_mtime
            except Exception:
                continue
            yield (mtime, Path(folder / entry.name))
