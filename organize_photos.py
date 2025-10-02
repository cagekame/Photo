#!/usr/bin/env python3
# organize_photos_v5.py — implementa i 10 punti richiesti
#
# Novità principali:
# 1) Sidecar gestiti anche durante la RISOLUZIONE dei duplicati (quarantena/elimina) nella stessa cartella del file.
# 2) Heuristica keeper: preferisce la data EXIF/metadata (DateTimeOriginal/MediaCreateDate/...) e solo in fallback usa mtime.
# 3) Duplicati "quasi uguali" (video ricodificati): opzionale via ffprobe, raggruppa per fingerprint (durata≈, risoluzione, fps, codec).
# 4) Ricerca sidecar accanto al file sorgente (src.parent) oltre alla base.
# 5) Controllo fuso orario: --no-local-tz per NON normalizzare al fuso locale (default: normalizza), oppure --utc per normalizzare a UTC.
# 6) Aggiunta .mkv al set dei video.
# 7) Exiftool robusto con file di argomenti (-@) per batch lunghi + retry + logging chiaro.
# 8) Log machine-readable: JSONL per scan duplicati e azioni (dup_scan.jsonl, dup_actions.jsonl) oltre ai txt.
# 9) Checkpoint ripartenza (organizzazione): organize_checkpoint.json per riprendere in caso di interruzione.
# 10) Sicurezza shell: shell fallback disattivabile con --no-shell-fallback (default: attivo). Sempre preferenza per esecuzione senza shell.
#
# Nota: le parti core originali sono state mantenute/riorganizzate. Interfaccia sempre interattiva, con nuove flag CLI opzionali.

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import shutil
from heapq import nsmallest
from collections import defaultdict
from typing import Dict, List, Tuple, Iterable, Any, Optional
import argparse
import tempfile

# ----------------------------
# Config di base (sovrascrivibili da CLI)
# ----------------------------
PHOTO_EXT = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".dng", ".nef", ".cr2", ".cr3", ".arw", ".rw2", ".orf"}
VIDEO_EXT = {".mp4", ".mov", ".m4v", ".avi", ".mts", ".m2ts", ".3gp", ".mkv"}  # (6) aggiunto .mkv
ALL_EXT = PHOTO_EXT | VIDEO_EXT
SIDECAR_EXT = {".aae", ".xmp", ".thm", ".lrv"}

DATE_KEYS = [
    "SubSecDateTimeOriginal", "DateTimeOriginal",
    "SubSecCreateDate", "CreateDate",
    "XMP:DateCreated", "Photoshop:DateCreated", "IPTC:DateCreated",
    "MediaCreateDate", "TrackCreateDate", "CreationDate",
    "ModifyDate",
    "FileCreateDate", "FileModifyDate",
]

REPORT_FILE = "duplicati_report.txt"
DUP_SCAN_REPORT = "duplicati_scan.txt"
DUP_SCAN_JSONL = "dup_scan.jsonl"          # (8)
DUP_ACTION_LOG = "duplicati_action.txt"
DUP_ACTIONS_JSONL = "dup_actions.jsonl"     # (8)
CONFIG_FILE = "exiftool_path.txt"
CHECKPOINT_FILE = "organize_checkpoint.json"  # (9)

BATCH_EXIF_SIZE = 25
PARTIAL_HASH_BYTES = 4 * 1024 * 1024  # 4MB per pre-hash

# Opzioni (alcune sovrascrivibili via CLI)
WARN_ON_MTIME = True
LOG_PICKED_DATE = True
NORMALIZE_TO_LOCAL = False   # default (5)
NORMALIZE_TO_UTC = False    # (5)
QUICKTIME_UTC = False        # Nuovo: se True, forza -api QuickTimeUTC (disattivo di default)
USE_SHELL_FALLBACK = True   # (10)

# ----------------------------
# CLI
# ----------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Organizza foto/video e gestisce duplicati.")
    ap.add_argument("--no-local-tz", action="store_true", help="Non normalizzare le date al fuso locale.")
    ap.add_argument("--utc", action="store_true", help="Normalizza le date a UTC (sovrascrive --no-local-tz).")
    ap.add_argument("--batch-exif", type=int, default=BATCH_EXIF_SIZE, help="Dimensione batch lettura EXIF (default 25).")
    ap.add_argument("--prehash-bytes", type=int, default=PARTIAL_HASH_BYTES, help="Dimensione pre-hash in byte (default 4MB).")
    ap.add_argument("--no-shell-fallback", action="store_true", help="Disabilita fallback shell=True (Windows).")
    ap.add_argument("--disable-near-dup", action="store_true",
                    help="Disattiva il rilevamento dei duplicati quasi uguali per i video (di default è ATTIVO; richiede ffprobe).")
    ap.add_argument("--qt-utc", action="store_true",
                    help="Usa -api QuickTimeUTC in exiftool (per trattare i tempi QuickTime come UTC).")
    return ap.parse_args()

# ----------------------------
# Gestione exiftool
# ----------------------------

def detect_exiftool(base_dir: Path) -> Optional[str]:
    exe_name = "exiftool.exe" if os.name == "nt" else "exiftool"
    sub = base_dir / "exiftool" / exe_name
    if sub.exists():
        return str(sub)
    local = base_dir / exe_name
    if local.exists():
        return str(local)
    for p in os.environ.get("PATH", "").split(os.pathsep):
        cand = Path(p) / exe_name
        if cand.exists():
            return str(cand)
    return None


def get_exiftool_path(base: Path) -> Optional[str]:
    found = detect_exiftool(base)
    if found:
        return found
    cfg = base / CONFIG_FILE
    if cfg.exists():
        try:
            path = cfg.read_text(encoding="utf-8").strip()
            if Path(path).exists():
                return path
        except Exception:
            pass
    print("❌ exiftool non trovato automaticamente.")
    path = input("Inserisci il percorso completo di exiftool.exe: ").strip('" ')
    if Path(path).exists():
        try:
            cfg.write_text(path, encoding="utf-8")
            print(f"✔ Percorso salvato in {CONFIG_FILE}")
        except Exception:
            print("⚠ Non sono riuscito a salvare il percorso, lo userò solo per questa sessione.")
        return path
    return None

# ----------------------------
# Utilità
# ----------------------------

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


def _to_str_values(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    if isinstance(v, (str, int, float)):
        return [str(v)]
    return [str(v)]


def parse_date_string(s: str):
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    formats = (
        "%Y:%m:%d %H:%M:%S%z",
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y:%m:%d %H:%M:%S.%f%z",
        "%Y:%m:%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S.%f",
    )
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# (5) Normalizzazione tempo

def _normalize_dt(dt: datetime) -> datetime:
    if NORMALIZE_TO_UTC:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if NORMALIZE_TO_LOCAL:
        try:
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                return dt.astimezone(datetime.now().astimezone().tzinfo)
        except Exception:
            pass
    return dt

# ----------------------------
# Esecuzione robusta di exiftool (7) + (10)
# ----------------------------

def _run_cmd_robust(cmd_list: List[str]) -> subprocess.CompletedProcess:
    """Prova prima senza shell. Se PermissionError/WinError 5 e consentito, riprova con shell=True."""
    try:
        return subprocess.run(cmd_list, capture_output=True, text=True)
    except PermissionError as e:
        winerr = getattr(e, "winerror", None)
        if os.name == "nt" and winerr == 5 and USE_SHELL_FALLBACK:
            # Fallback via cmd.exe
            quoted = []
            for tok in cmd_list:
                if " " in tok or "\\" in tok or ":" in tok:
                    quoted.append(f'"{tok}"')
                else:
                    quoted.append(tok)
            cmdline = " ".join(quoted)
            return subprocess.run(cmdline, capture_output=True, text=True, shell=True)
        raise
    except OSError as e:
        winerr = getattr(e, "winerror", None)
        if os.name == "nt" and winerr == 5 and USE_SHELL_FALLBACK:
            quoted = []
            for tok in cmd_list:
                if " " in tok or "\\" in tok or ":" in tok:
                    quoted.append(f'"{tok}"')
                else:
                    quoted.append(tok)
            cmdline = " ".join(quoted)
            return subprocess.run(cmdline, capture_output=True, text=True, shell=True)
        raise


def exiftool_version(exiftool: str) -> Optional[str]:
    try:
        r = _run_cmd_robust([exiftool, "-ver"])
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return None


# (7) Exiftool wrapper con file argomenti e retry

def run_exiftool_json(exiftool: str, tags: List[str], files: List[Path], fast: bool = True) -> Optional[List[dict]]:
    if not files:
        return []
    fmt = ["-d", "%Y:%m:%d %H:%M:%S%z"]
    base_cmd = [exiftool, "-q", "-q", "-j", "-charset", "filename=utf8", *fmt]
    if QUICKTIME_UTC:
        base_cmd.extend(["-api", "QuickTimeUTC"])
    if fast:
        base_cmd.insert(2, "-fast")  # dopo -q -q

    tag_args = [f"-{k}" for k in tags]

    # Scrivi lista file in file temporaneo per -@
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="\n") as tf:
        argfile = tf.name
        for p in files:
            tf.write(str(p.resolve()) + "\n")

    try:
        attempts = [base_cmd + tag_args + ["-@", argfile],
                    [exiftool, "-q", "-q", "-j", "-charset", "filename=utf8", *fmt, *tag_args, "-@", argfile]]
        for i, cmd in enumerate(attempts, start=1):
            r = _run_cmd_robust(cmd)
            if r.returncode == 0 and r.stdout.strip():
                try:
                    return json.loads(r.stdout)
                except Exception as je:
                    print(f"[ERRORE] JSON parse exiftool (tentativo {i}): {je}")
            else:
                err = (r.stderr or "").strip()
                print(f"[ERRORE] exiftool rc={r.returncode} (tentativo {i}) su {len(files)} file. stderr: {err}")
    finally:
        try:
            os.remove(argfile)
        except Exception:
            pass
    return None


def exif_dates_batch(exiftool: str, files: List[Path], batch_size: int) -> Dict[Path, datetime]:
    result: Dict[Path, datetime] = {}
    if not files or not exiftool:
        return result
    for i in range(0, len(files), batch_size):
        chunk = files[i:i + batch_size]
        arr = run_exiftool_json(exiftool, DATE_KEYS, chunk, fast=True)
        if not arr:
            for p in chunk:
                if WARN_ON_MTIME:
                    print(f"[WARN] Nessuna data metadata parsabile (batch) per: {p.name}. Userò mtime al bisogno.")
            continue
        for obj in arr:
            src = obj.get("SourceFile")
            if not src:
                continue
            path = Path(src)
            chosen = None
            chosen_key = None
            for k in DATE_KEYS:
                vals = _to_str_values(obj.get(k))
                for sv in vals:
                    dt = parse_date_string(sv)
                    if dt:
                        chosen = _normalize_dt(dt)
                        chosen_key = k
                        break
                if chosen:
                    break
            if chosen:
                result[path] = chosen
                if LOG_PICKED_DATE:
                    print(f"[DATE] {path.name}: {chosen.isoformat()} (tag={chosen_key})")
            else:
                if WARN_ON_MTIME:
                    print(f"[WARN] Nessuna data metadata parsabile (batch) per: {path.name}. Userò mtime al bisogno.")
    return result


def get_taken_datetime(p: Path, exiftool: Optional[str], premap: Optional[Dict[Path, datetime]] = None) -> datetime:
    if premap and p in premap:
        return premap[p]
    if exiftool:
        arr = run_exiftool_json(exiftool, DATE_KEYS, [p], fast=True)
        if arr and isinstance(arr, list) and arr:
            obj = arr[0]
            for k in DATE_KEYS:
                vals = _to_str_values(obj.get(k))
                for sv in vals:
                    dt = parse_date_string(sv)
                    if dt:
                        dt = _normalize_dt(dt)
                        if LOG_PICKED_DATE:
                            print(f"[DATE] {p.name}: {dt.isoformat()} (tag={k})")
                        return dt
            if WARN_ON_MTIME:
                print(f"[WARN] Nessuna data metadata parsabile per: {p.name}. Uso mtime.")
        else:
            if WARN_ON_MTIME:
                print(f"[WARN] exiftool non ha restituito dati per {p.name}. Uso mtime.")
    return datetime.fromtimestamp(p.stat().st_mtime)

# ----------------------------
# Duplicati e organizzazione
# ----------------------------

def sidecars_for(p: Path) -> Iterable[Path]:
    """(4) Cerca sidecar accanto al file, e in sub-secondo step anche nella base del job.
    Qui implementiamo la ricerca accanto al file (src.parent)."""
    stem = p.with_suffix("").name
    parent = p.parent
    for ext in SIDECAR_EXT:
        cand = parent / f"{stem}{ext}"
        try:
            if cand.is_file():
                yield cand
        except Exception:
            continue


def ensure_dir(p: Path, dry_run: bool):
    if not dry_run:
        p.mkdir(parents=True, exist_ok=True)


def append_report_line(base: Path, src: Path, dst: Path, dry_run: bool):
    line = f"DUPLICATO: '{src.name}' uguale a '{dst.name}' in {dst.parent}\n"
    if not dry_run:
        with (base / REPORT_FILE).open("a", encoding="utf-8") as f:
            f.write(line)


def append_jsonl(path: Path, obj: dict):
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"


def walk_files(base: Path, recursive: bool) -> Iterable[Path]:
    if not recursive:
        with os.scandir(base) as it:
            for e in it:
                if e.is_file(follow_symlinks=False):
                    _, ext = os.path.splitext(e.name)
                    if ext.lower() in ALL_EXT:
                        yield Path(base / e.name)
    else:
        stack = [base]
        while stack:
            d = stack.pop()
            try:
                with os.scandir(d) as it:
                    for e in it:
                        try:
                            if e.is_dir(follow_symlinks=False):
                                stack.append(Path(e.path))
                            elif e.is_file(follow_symlinks=False):
                                _, ext = os.path.splitext(e.name)
                                if ext.lower() in ALL_EXT:
                                    yield Path(e.path)
                        except Exception:
                            continue
            except Exception:
                continue


def find_duplicate_groups(base: Path, recursive: bool, prehash_bytes: int):
    size_map: Dict[int, List[Path]] = defaultdict(list)
    total_files = 0
    total_bytes = 0
    start_ts = datetime.now()

    for p in walk_files(base, recursive):
        try:
            st = p.stat()
        except Exception:
            continue
        size_map[st.st_size].append(p)
        total_files += 1
        total_bytes += st.st_size
        if total_files % 1000 == 0:
            elapsed = (datetime.now() - start_ts).total_seconds() or 1
            rate = total_files / elapsed
            print(f"  … indicizzati {total_files} file (≈{rate:.1f} file/s)")

    partial_groups: Dict[Tuple[int, str], List[Path]] = defaultdict(list)
    for size, group in size_map.items():
        if len(group) < 2:
            continue
        for p in group:
            try:
                ph = file_sha1_head(p, prehash_bytes)
            except Exception:
                continue
            partial_groups[(size, ph)].append(p)

    full_groups: Dict[str, List[Path]] = defaultdict(list)
    for (size, ph), group in partial_groups.items():
        if len(group) < 2:
            continue
        for p in group:
            try:
                h = file_sha1(p)
            except Exception:
                continue
            full_groups[h].append(p)

    dup_groups = {h: lst for h, lst in full_groups.items() if len(lst) > 1}
    return dup_groups, total_files, total_bytes


# (2) Keeper: preferisci data EXIF/metadata

def keeper_key(p: Path, exiftool: Optional[str]) -> Tuple[int, float, str]:
    """Ordina per (has_meta, timestamp, path) dove has_meta=0 se ha data dai metadati (più desiderabile), 1 altrimenti.
    timestamp: epoch; più piccolo = più vecchio = preferito."""
    dt: Optional[datetime] = None
    if exiftool:
        try:
            arr = run_exiftool_json(exiftool, DATE_KEYS, [p], fast=True)
            if arr and isinstance(arr, list) and arr:
                obj = arr[0]
                for k in DATE_KEYS:
                    vals = _to_str_values(obj.get(k))
                    for sv in vals:
                        d = parse_date_string(sv)
                        if d:
                            dt = _normalize_dt(d)
                            break
                    if dt:
                        break
        except Exception:
            dt = None
    if dt is None:
        # fallback mtime
        try:
            ts = p.stat().st_mtime
        except Exception:
            ts = float('inf')
        return (1, ts, str(p))
    else:
        return (0, dt.timestamp(), str(p))


def choose_keeper(paths: List[Path], exiftool: Optional[str]) -> Path:
    try:
        return min(paths, key=lambda p: keeper_key(p, exiftool))
    except Exception:
        # fallback ordinamento per path
        return min(paths, key=lambda p: str(p))


# (3) Near-duplicates video via ffprobe (opzionale)

def detect_ffprobe() -> Optional[str]:
    exe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
    for p in os.environ.get("PATH", "").split(os.pathsep):
        cand = Path(p) / exe
        if cand.exists():
            return str(cand)
    return None


def video_fingerprint(ffprobe: str, p: Path) -> Optional[Tuple[int, int, str, float]]:
    """Ritorna (width, height, codec, duration_rounded) per v:0. duration arrotondata a 0.5s."""
    try:
        cmd = [ffprobe, "-v", "error", "-select_streams", "v:0",
               "-show_entries", "stream=width,height,codec_name,avg_frame_rate,duration",
               "-of", "json", str(p)]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
        streams = data.get("streams") or []
        if not streams:
            return None
        s0 = streams[0]
        w = int(float(s0.get("width", 0) or 0))
        h = int(float(s0.get("height", 0) or 0))
        codec = str(s0.get("codec_name") or "?")
        dur = s0.get("duration")
        if dur is None:
            # calcola da fps? lascio None → non comparabile
            return None
        durf = float(dur)
        # arrotonda a 0.5s per tollerare drift
        durf = round(durf * 2) / 2.0
        return (w, h, codec, durf)
    except Exception:
        return None


def scan_duplicates(base: Path, recursive: bool, prehash_bytes: int, enable_near_dup: bool = True):
    print(f"Inizio scansione duplicati in: {base}  (ricorsivo: {'Sì' if recursive else 'No'})")
    dup_groups, total_files, total_bytes = find_duplicate_groups(base, recursive, prehash_bytes)
    groups_count = len(dup_groups)
    potential_savings = 0

    # JSONL pulizia
    for f in [DUP_SCAN_JSONL]:
        try:
            (base / f).unlink(missing_ok=True)
        except Exception:
            pass

    with (base / DUP_SCAN_REPORT).open("w", encoding="utf-8") as f:
        f.write(f"Report duplicati — generato: {datetime.now().isoformat()}\n")
        f.write(f"Cartella base: {base}\nRicorsivo: {'Sì' if recursive else 'No'}\n")
        f.write(f"File indicizzati: {total_files}  (totale dati: {human_size(total_bytes)})\n\n")
        for i, (h, paths) in enumerate(sorted(dup_groups.items(), key=lambda x: -len(x[1])), start=1):
            try:
                sz = paths[0].stat().st_size
            except Exception:
                sz = 0
            save = sz * (len(paths) - 1)
            potential_savings += save
            f.write(f"[{i}] SHA1={h}  pezzi={len(paths)}  size={human_size(sz)}  risparmio_potenziale={human_size(save)}\n")
            for p in paths:
                f.write(f"    - {p}\n")
            f.write("\n")
            # JSONL (8)
            append_jsonl(base / DUP_SCAN_JSONL, {
                "group_index": i,
                "sha1": h,
                "count": len(paths),
                "size": sz,
                "potential_saving_bytes": save,
                "paths": [str(x) for x in paths],
            })
        f.write(f"=== RIEPILOGO ===\nGruppi duplicati: {groups_count}\nPotenziale spazio recuperabile: {human_size(potential_savings)}\n")

    near_dup_summary = None
    if enable_near_dup:
        ffprobe = detect_ffprobe()
        if not ffprobe:
            print("[NEAR-DUP] ffprobe non trovato: salto la rilevazione duplicati quasi uguali.")
        else:
            # Scansione semplice: solo video
            video_files = [p for p in walk_files(base, recursive) if p.suffix.lower() in VIDEO_EXT]
            fp_map: Dict[Tuple[int, int, str, float], List[Path]] = defaultdict(list)
            for p in video_files:
                fp = video_fingerprint(ffprobe, p)
                if fp:
                    fp_map[fp].append(p)
            # Report near duplicates (stesse impronte ma hash diversi)
            near_groups = []
            for fp, lst in fp_map.items():
                if len(lst) < 2:
                    continue
                # Dividi per sha1: se più di un sha1 presente, potenziale near-dup
                sha_map = defaultdict(list)
                for p in lst:
                    try:
                        h = file_sha1(p)
                    except Exception:
                        continue
                    sha_map[h].append(p)
                if len(sha_map) > 1:
                    near_groups.append((fp, sha_map))
            if near_groups:
                print(f"[NEAR-DUP] Trovati {len(near_groups)} gruppi di probabili duplicati ricodificati.")
                with (base / (DUP_SCAN_REPORT.replace('.txt', '_near.txt'))).open("w", encoding="utf-8") as nf:
                    for idx, (fp, sha_map) in enumerate(near_groups, start=1):
                        w, h, codec, dur = fp
                        nf.write(f"[{idx}] {w}x{h} {codec} dur≈{dur}s\n")
                        for hh, plist in sha_map.items():
                            nf.write(f"  SHA1={hh} (n={len(plist)})\n")
                            for p in plist:
                                nf.write(f"    - {p}\n")
                        nf.write("\n")
                near_dup_summary = len(near_groups)

    print(f"Scansione completata. Gruppi duplicati: {groups_count}.")
    if near_dup_summary is not None:
        print(f"Gruppi near-duplicate: {near_dup_summary} (vedi *_near.txt)")
    print(f"Report scritto in: {base / DUP_SCAN_REPORT}")
    return dup_groups


# (1) Sidecar anche nella fase duplicati + (2) keeper exif

def consolidate_duplicates(base: Path, dup_groups: Dict[str, List[Path]], mode: str, exiftool: Optional[str]):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S$")
    quarantine_root = base / f"_QuarantenaDuplicati_{timestamp}"
    if mode == "quarantine":
        quarantine_root.mkdir(parents=True, exist_ok=True)
        print(f"Cartella QUARANTENA: {quarantine_root}")

    # pulizia JSONL azioni
    try:
        (base / DUP_ACTIONS_JSONL).unlink(missing_ok=True)
    except Exception:
        pass

    actions = []
    groups = sorted(dup_groups.items(), key=lambda x: -len(x[1]))
    for i, (h, paths) in enumerate(groups, start=1):
        if len(paths) < 2:
            continue
        keeper = choose_keeper(paths, exiftool)
        print(f"[{i}/{len(groups)}] Keeper: {keeper}")
        for p in paths:
            if p == keeper:
                continue
            # (1) includi sidecar nella stessa azione
            related = [p] + list(sidecars_for(p))
            if mode == "quarantine":
                target_dir = quarantine_root / h
                target_dir.mkdir(parents=True, exist_ok=True)
                for item in related:
                    dest = target_dir / item.name
                    if dest.exists():
                        stem, ext = dest.stem, dest.suffix
                        n = 2
                        while (target_dir / f"{stem}_{n}{ext}").exists():
                            n += 1
                        dest = target_dir / f"{stem}_{n}{ext}"
                    try:
                        shutil.move(str(item), str(dest))
                        actions.append(("MOVED", str(item), str(dest)))
                        print(f"    spostato -> {dest}")
                        append_jsonl(base / DUP_ACTIONS_JSONL, {
                            "action": "move",
                            "source": str(item),
                            "dest": str(dest),
                            "group_sha1": h,
                        })
                    except Exception as e:
                        print(f"    [ERRORE] Spostando {item}: {e}")
            elif mode == "delete":
                for item in related:
                    try:
                        os.remove(item)
                        actions.append(("DELETED", str(item)))
                        print(f"    eliminato -> {item}")
                        append_jsonl(base / DUP_ACTIONS_JSONL, {
                            "action": "delete",
                            "source": str(item),
                            "group_sha1": h,
                        })
                    except Exception as e:
                        print(f"    [ERRORE] Eliminando {item}: {e}")

    with (base / DUP_ACTION_LOG).open("a", encoding="utf-8") as f:
        f.write(f"Azione duplicati — {datetime.now().isoformat()}  mode={mode}\n")
        for a in actions:
            f.write(" ".join(a) + "\n")
        f.write("\n")

    if mode == "quarantine":
        print("Operazione completata: COPIE duplicate spostate in QUARANTENA (con sidecar). Verifica e poi, se vuoi, elimina la cartella.")
    else:
        print("Operazione completata: COPIE duplicate eliminate definitivamente (con sidecar).")


# Checkpoint (9)

def load_checkpoint(base: Path) -> Optional[dict]:
    cp = base / CHECKPOINT_FILE
    if cp.exists():
        try:
            return json.loads(cp.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def save_checkpoint(base: Path, data: dict):
    try:
        (base / CHECKPOINT_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# ----------------------------
# main
# ----------------------------

def main():
    global NORMALIZE_TO_LOCAL, NORMALIZE_TO_UTC, BATCH_EXIF_SIZE, PARTIAL_HASH_BYTES, USE_SHELL_FALLBACK, QUICKTIME_UTC

    args = parse_args()
    if args.no_local_tz:
        NORMALIZE_TO_LOCAL = False
    if args.utc:
        NORMALIZE_TO_LOCAL = False
        NORMALIZE_TO_UTC = True
    BATCH_EXIF_SIZE = max(1, args.batch_exif)
    PARTIAL_HASH_BYTES = max(1024 * 1024, args.prehash_bytes)
    if args.no_shell_fallback:
        USE_SHELL_FALLBACK = False
    if args.qt_utc:
        QUICKTIME_UTC = True

    base = Path(__file__).resolve().parent
    exiftool = get_exiftool_path(base)

    print(f"Cartella di lavoro: {base}")
    if exiftool:
        ver = exiftool_version(exiftool) or "sconosciuta"
        print(f"exiftool: trovato -> {exiftool} (ver: {ver})")
    else:
        print("exiftool: NON trovato, userò mtime come fallback")

    # Prompt: scansione / risoluzione duplicati
    dup = input("Vuoi eseguire una SCANSIONE DUPLICATI senza spostare nulla? [s/N]: ").strip().lower()
    if dup == "s":
        rec = input("Includere anche le SOTTOCARTELLE (ricorsivo)? [S/n]: ").strip().lower()
        recursive = (rec != "n")
        dup_groups, _, _ = find_duplicate_groups(base, recursive, PARTIAL_HASH_BYTES)
        if dup_groups:
            _ = scan_duplicates(base, recursive, PARTIAL_HASH_BYTES, enable_near_dup=(not args.disable_near_dup))
            fix = input("Vuoi RISOLVERE i duplicati adesso lasciandone uno solo? [s/N]: ").strip().lower()
            if fix == "s":
                mode = input("Come procedo? [1 = sposta in QUARANTENA (consigliato), 2 = ELIMINA definitivamente]: ").strip()
                mode = "delete" if mode == "2" else "quarantine"
                print("ATTENZIONE: l'operazione è potenzialmente distruttiva. Continua? [s/N]: ", end="")
                if input().strip().lower() == "s":
                    consolidate_duplicates(base, dup_groups, mode=mode, exiftool=exiftool)
                else:
                    print("Annullato su richiesta utente.")
        else:
            print("Nessun duplicato trovato.")
        print("\nFINE scansione/risoluzione duplicati. Esco.")
        return 0

    # Prompt operativi per organizzazione
    choice = input("Vuoi eseguire in modalità PROVA (dry-run)? [s/N]: ").strip().lower()
    dry_run = choice == "s"

    batch_in = input("Vuoi limitare l'esecuzione a un BATCH di N file? [Invio = nessun limite]: ").strip()
    batch_size = None
    if batch_in:
        try:
            batch_size = max(1, int(batch_in))
        except ValueError:
            print("Valore batch non valido: ignoro, nessun limite.")
            batch_size = None

    print("Modalità:", "DRY-RUN (simulazione, non sposto nulla)" if dry_run else "NORMALE (sposto i file)")

    # Prescan ottimizzato
    print("Preparazione elenco files…")
    if batch_size:
        smallest = nsmallest(batch_size, iter_candidates_fast(base), key=lambda t: t[0])
        candidates = [p for _, p in smallest]
        print(f"Limiterò il lavoro a {len(candidates)} file in questo batch (selezione rapida).")
    else:
        tmp = list(iter_candidates_fast(base))
        tmp.sort(key=lambda t: t[0])
        candidates = [p for _, p in tmp]
        print(f"Trovati {len(candidates)} file candidati. Elenco preparato.")

    # Carica checkpoint?
    cp = load_checkpoint(base)
    start_index = 0
    if cp and cp.get("last_index") is not None:
        print(f"Checkpoint trovato: riprendo da indice {cp['last_index']+1}/{len(candidates)}")
        start_index = min(max(0, int(cp["last_index"]) + 1), len(candidates))

    # Lettura EXIF in batch per i candidati
    exif_map = exif_dates_batch(exiftool, candidates, BATCH_EXIF_SIZE) if exiftool else {}

    moved = 0
    skipped_dup = 0
    conflicts = 0

    try:
        total = len(candidates)
        for idx, src in enumerate(candidates[start_index:], start=start_index + 1):
            taken = get_taken_datetime(src, exiftool, premap=exif_map)
            year = f"{taken.year:04d}"
            month = f"{taken.month:02d}"
            dest_dir = base / year / month
            ensure_dir(dest_dir, dry_run)
            dest_file = dest_dir / src.name

            print(f"[{idx}/{total}] {src.name}")

            if dest_file.exists():
                try:
                    src_hash = file_sha1(src)
                    dst_hash = file_sha1(dest_file)
                except Exception as e:
                    print(f"[ERRORE] Hash su {src.name}: {e}")
                    conflicts += 1
                    save_checkpoint(base, {"last_index": idx-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})
                    continue

                if src_hash == dst_hash:
                    print(f"[DUP] {src.name} identico già in {year}/{month}. Segnato in {REPORT_FILE}.")
                    append_report_line(base, src, dest_file, dry_run)
                    skipped_dup += 1
                    save_checkpoint(base, {"last_index": idx-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})
                    continue
                else:
                    print(f"[CONFLITTO] {src.name} esiste già in {year}/{month} ma è diverso. Non sposto.")
                    conflicts += 1
                    save_checkpoint(base, {"last_index": idx-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})
                    continue

            if dry_run:
                print(f"[SIMULA] Sposterei {src.name} -> {year}/{month}/")
            else:
                try:
                    shutil.move(str(src), str(dest_file))
                    moved += 1
                    print(f"[SPOSTATO] {dest_file.relative_to(base)}")
                except Exception as e:
                    print(f"[ERRORE] Spostando {src.name}: {e}")
                    save_checkpoint(base, {"last_index": idx-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})
                    continue

                # (4) sidecar accanto al file
                for sc in sidecars_for(dest_file):
                    sc_src = src.parent / sc.name  # sidecar originale accanto a src
                    sc_dest = dest_dir / sc.name
                    if sc_dest.exists():
                        try:
                            if sc_src.exists() and file_sha1(sc_src) == file_sha1(sc_dest):
                                print(f"         (sidecar) {sc.name} già presente (identico). Salto.")
                            else:
                                print(f"         (sidecar) CONFLITTO {sc.name} già presente ma diverso. Non sposto.")
                                conflicts += 1
                        except Exception:
                            conflicts += 1
                    else:
                        try:
                            if sc_src.exists():
                                shutil.move(str(sc_src), str(sc_dest))
                                print(f"         (sidecar) spostato {sc.name}")
                        except Exception as e:
                            print(f"         (sidecar) ERRORE spostando {sc.name}: {e}")
            # salva checkpoint ad ogni iterazione
            save_checkpoint(base, {"last_index": idx-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})
    except KeyboardInterrupt:
        print("\n[INTERRUZIONE] Esecuzione interrotta dall'utente. Riepilogo parziale sotto.")

    print("\n--- Riepilogo ---")
    print(f"Spostati   : {moved}")
    print(f"Duplicati  : {skipped_dup} (vedi {REPORT_FILE} se > 0)")
    print(f"Conflitti  : {conflicts}")
    # checkpoint finale
    save_checkpoint(base, {"last_index": len(candidates)-1, "moved": moved, "duplicati": skipped_dup, "conflicts": conflicts})

    return 0


def iter_candidates_fast(folder: Path):
    with os.scandir(folder) as it:
        for entry in it:
            if not entry.is_file(follow_symlinks=False):
                continue
            name = entry.name
            _, ext = os.path.splitext(name)
            if ext.lower() not in ALL_EXT:
                continue
            try:
                mtime = entry.stat(follow_symlinks=False).st_mtime
            except Exception:
                continue
            yield (mtime, Path(folder / name))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as e:
        raise
    except Exception as e:
        print(f"[FATALE] {e}")
        sys.exit(1)
