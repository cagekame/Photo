"""Tutto ciò che riguarda exiftool: ricerca, esecuzione e lettura delle date."""
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import CONFIG_FILE, DATE_KEYS, settings


# ----------------------------
# Ricerca exiftool
# ----------------------------

def detect_exiftool(base_dir: Path) -> Optional[str]:
    exe_name = "exiftool.exe" if os.name == "nt" else "exiftool"
    for cand in (base_dir / "exiftool" / exe_name, base_dir / exe_name):
        if cand.exists():
            return str(cand)
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
            if path and Path(path).is_file():
                return path
        except Exception:
            pass
    print("❌ exiftool non trovato automaticamente: scegli il file nella finestra.")
    from .dialogs import choose_file
    exe = "exiftool.exe" if os.name == "nt" else "exiftool"
    path = choose_file("Dove si trova exiftool? (Annulla = continua senza, userò la data dei file)",
                       [("exiftool", exe), ("Tutti i file", "*")])
    if path and Path(path).is_file():
        try:
            cfg.write_text(path, encoding="utf-8")
            print(f"✔ Percorso salvato in {CONFIG_FILE}")
        except Exception:
            print("⚠ Non sono riuscito a salvare il percorso, lo userò solo per questa sessione.")
        return path
    return None


# ----------------------------
# Esecuzione robusta
# ----------------------------

def _shell_cmdline(cmd_list: List[str]) -> str:
    quoted = []
    for tok in cmd_list:
        if " " in tok or "\\" in tok or ":" in tok:
            quoted.append(f'"{tok}"')
        else:
            quoted.append(tok)
    return " ".join(quoted)


def run_cmd_robust(cmd_list: List[str]) -> subprocess.CompletedProcess:
    """Prova senza shell; su Windows, se WinError 5 e consentito, riprova con shell=True."""
    try:
        return subprocess.run(cmd_list, capture_output=True, text=True)
    except OSError as e:  # PermissionError è sottoclasse di OSError
        if os.name == "nt" and getattr(e, "winerror", None) == 5 and settings.use_shell_fallback:
            return subprocess.run(_shell_cmdline(cmd_list), capture_output=True, text=True, shell=True)
        raise


def exiftool_version(exiftool: str) -> Optional[str]:
    try:
        r = run_cmd_robust([exiftool, "-ver"])
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return None


def _write_argfile(files: List[Path]) -> str:
    """exiftool legge i percorsi da file (-@): evita limiti di riga di comando e problemi con caratteri non ASCII."""
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="\n") as tf:
        for p in files:
            tf.write(str(p.resolve()) + "\n")
        return tf.name


def run_exiftool_json(exiftool: str, tags: List[str], files: List[Path], fast: bool = True) -> Optional[List[dict]]:
    if not files:
        return []
    fmt = ["-d", "%Y:%m:%d %H:%M:%S%z"]
    base_cmd = [exiftool, "-q", "-q", "-j", "-charset", "filename=utf8", *fmt]
    if settings.quicktime_utc:
        base_cmd.extend(["-api", "QuickTimeUTC"])
    if fast:
        base_cmd.insert(2, "-fast")
    tag_args = [f"-{k}" for k in tags]

    argfile = _write_argfile(files)
    try:
        attempts = [base_cmd + tag_args + ["-@", argfile],
                    [exiftool, "-q", "-q", "-j", "-charset", "filename=utf8", *fmt, *tag_args, "-@", argfile]]
        for i, cmd in enumerate(attempts, start=1):
            r = run_cmd_robust(cmd)
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


def extract_binary_tag(exiftool: str, path: Path, tag: str) -> Optional[bytes]:
    """Estrae un tag binario (es. PreviewImage di un RAW). Ritorna None se assente."""
    argfile = _write_argfile([path])
    try:
        r = subprocess.run([exiftool, "-q", "-q", "-b", "-charset", "filename=utf8", f"-{tag}", "-@", argfile],
                           capture_output=True, timeout=30)
        return r.stdout if r.returncode == 0 and r.stdout else None
    except Exception:
        return None
    finally:
        try:
            os.remove(argfile)
        except Exception:
            pass


def read_orientation(exiftool: str, path: Path) -> Optional[int]:
    """Orientamento EXIF numerico (1-8) del file, usato per ruotare le anteprime estratte dai RAW."""
    arr = run_exiftool_json(exiftool, ["Orientation#"], [path], fast=True)
    try:
        return int(arr[0].get("Orientation")) if arr else None
    except Exception:
        return None


# ----------------------------
# Date
# ----------------------------

def _to_str_values(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    return [str(v)]


def parse_date_string(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    formats = (
        "%Y:%m:%d %H:%M:%S%z", "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S",
        "%Y:%m:%d %H:%M:%S.%f%z", "%Y:%m:%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S.%f",
    )
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def normalize_dt(dt: datetime) -> datetime:
    if settings.normalize_to_utc:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if settings.normalize_to_local:
        try:
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                return dt.astimezone(datetime.now().astimezone().tzinfo)
        except Exception:
            pass
    return dt


def _pick_date(obj: dict):
    for k in DATE_KEYS:
        for sv in _to_str_values(obj.get(k)):
            dt = parse_date_string(sv)
            if dt:
                return normalize_dt(dt), k
    return None, None


def exif_dates_batch(exiftool: str, files: List[Path], batch_size: int) -> Dict[Path, datetime]:
    result: Dict[Path, datetime] = {}
    if not files or not exiftool:
        return result
    for i in range(0, len(files), batch_size):
        chunk = files[i:i + batch_size]
        arr = run_exiftool_json(exiftool, DATE_KEYS, chunk, fast=True)
        if not arr:
            if settings.warn_on_mtime:
                for p in chunk:
                    print(f"[WARN] Nessuna data metadata parsabile (batch) per: {p.name}. Userò mtime al bisogno.")
            continue
        for obj in arr:
            src = obj.get("SourceFile")
            if not src:
                continue
            path = Path(src)
            chosen, key = _pick_date(obj)
            if chosen:
                result[path] = chosen
                if settings.log_picked_date:
                    print(f"[DATE] {path.name}: {chosen.isoformat()} (tag={key})")
            elif settings.warn_on_mtime:
                print(f"[WARN] Nessuna data metadata parsabile (batch) per: {path.name}. Userò mtime al bisogno.")
    return result


def get_taken_datetime(p: Path, exiftool: Optional[str], premap: Optional[Dict[Path, datetime]] = None) -> datetime:
    if premap and p in premap:
        return premap[p]
    if exiftool:
        arr = run_exiftool_json(exiftool, DATE_KEYS, [p], fast=True)
        if arr:
            dt, key = _pick_date(arr[0])
            if dt:
                if settings.log_picked_date:
                    print(f"[DATE] {p.name}: {dt.isoformat()} (tag={key})")
                return dt
            if settings.warn_on_mtime:
                print(f"[WARN] Nessuna data metadata parsabile per: {p.name}. Uso mtime.")
        elif settings.warn_on_mtime:
            print(f"[WARN] exiftool non ha restituito dati per {p.name}. Uso mtime.")
    return datetime.fromtimestamp(p.stat().st_mtime)
