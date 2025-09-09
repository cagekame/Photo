#!/usr/bin/env python3
# organize_photos.py — v4.9-exifcfg
# - exiftool: formato date forzato (-d "%Y:%m:%d %H:%M:%S%z")
# - Ricerca percorso + prompt + cache in exiftool_path.txt
# - Fix Windows: se WinError 5 (Accesso negato) -> fallback via cmd.exe (shell=True)
# - Log tag scelto; fallback a mtime se nessuna data disponibile
# - Duplicati (scan/quarantena/elimina) e organizzazione ANNO/MESE

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime
import hashlib
import shutil
from heapq import nsmallest
from collections import defaultdict
from typing import Dict, List, Tuple, Iterable, Any

# ----------------------------
# Config
# ----------------------------
PHOTO_EXT = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".dng", ".nef", ".cr2", ".cr3", ".arw", ".rw2", ".orf"}
VIDEO_EXT = {".mp4", ".mov", ".m4v", ".avi", ".mts", ".m2ts", ".3gp"}
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
DUP_ACTION_LOG = "duplicati_action.txt"
CONFIG_FILE = "exiftool_path.txt"

BATCH_EXIF_SIZE = 25
PARTIAL_HASH_BYTES = 4 * 1024 * 1024  # 4MB per pre-hash

# Opzioni
WARN_ON_MTIME = True
LOG_PICKED_DATE = True
NORMALIZE_TO_LOCAL = True

# ----------------------------
# Gestione exiftool (rilevamento + prompt + cache su file)
# ----------------------------
def detect_exiftool(base_dir: Path) -> str | None:
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

def get_exiftool_path(base: Path) -> str | None:
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

def file_sha1_head(p: Path, max_bytes: int = PARTIAL_HASH_BYTES) -> str:
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

def _normalize_to_local(dt: datetime) -> datetime:
    if not NORMALIZE_TO_LOCAL:
        return dt
    try:
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            return dt.astimezone(datetime.now().astimezone().tzinfo)
    except Exception:
        pass
    return dt

# ----------------------------
# Esecuzione robusta di exiftool (fix WinError 5)
# ----------------------------
def _run_cmd_robust(cmd_list: List[str]) -> subprocess.CompletedProcess:
    """
    Prova prima senza shell. Se PermissionError/WinError 5, riprova con shell=True
    (cmd.exe) costruendo una riga di comando quotata.
    """
    try:
        return subprocess.run(cmd_list, capture_output=True, text=True)
    except PermissionError as e:
        winerr = getattr(e, "winerror", None)
        if os.name == "nt" and winerr == 5:
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
        if os.name == "nt" and winerr == 5:
            quoted = []
            for tok in cmd_list:
                if " " in tok or "\\" in tok or ":" in tok:
                    quoted.append(f'"{tok}"')
                else:
                    quoted.append(tok)
            cmdline = " ".join(quoted)
            return subprocess.run(cmdline, capture_output=True, text=True, shell=True)
        raise

def exiftool_version(exiftool: str) -> str | None:
    try:
        r = _run_cmd_robust([exiftool, "-ver"])
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    return None

# ----------------------------
# Exiftool wrapper (con formato date forzato)
# ----------------------------
def run_exiftool_json(exiftool: str, tags: List[str], files: List[Path]) -> List[dict] | None:
    abs_files = [str(p.resolve()) for p in files]
    fmt = ["-d", "%Y:%m:%d %H:%M:%S%z"]

    attempts = [
        [exiftool, "-q", "-q", "-fast", "-j", "-charset", "filename=utf8", "-api", "QuickTimeUTC",
         *fmt, *[f"-{k}" for k in tags], *abs_files],
        [exiftool, "-q", "-q", "-j", "-charset", "filename=utf8",
         *fmt, *[f"-{k}" for k in tags], *abs_files],
    ]
    for i, cmd in enumerate(attempts, start=1):
        try:
            r = _run_cmd_robust(cmd)
            if r.returncode == 0 and r.stdout.strip():
                try:
                    return json.loads(r.stdout)
                except Exception as je:
                    print(f"[ERRORE] JSON parse exiftool (tentativo {i}): {je}")
            else:
                err = (r.stderr or "").strip()
                print(f"[ERRORE] exiftool rc={r.returncode} (tentativo {i}) su {len(files)} file. stderr: {err}")
        except Exception as e:
            msg = str(e)
            if os.name == "nt" and ("WinError 5" in msg or "Accesso negato" in msg):
                print("❌ ERRORE: Windows ha negato l'esecuzione di exiftool.exe (WinError 5).")
                print("   Suggerimenti: Sblocca il file, verifica i permessi NTFS della cartella,")
                print("   o consenti l'app in 'Accesso alle cartelle controllato' di Windows Defender.")
            else:
                print(f"[ERRORE] eseguendo exiftool (tentativo {i}): {e}")
    return None

def exif_dates_batch(exiftool: str, files: List[Path]) -> Dict[Path, datetime]:
    result: Dict[Path, datetime] = {}
    if not files or not exiftool:
        return result
    for i in range(0, len(files), BATCH_EXIF_SIZE):
        chunk = files[i:i + BATCH_EXIF_SIZE]
        arr = run_exiftool_json(exiftool, DATE_KEYS, chunk)
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
                        chosen = _normalize_to_local(dt)
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

def get_taken_datetime(p: Path, exiftool: str | None, premap: Dict[Path, datetime] | None = None) -> datetime:
    if premap and p in premap:
        return premap[p]
    if exiftool:
        arr = run_exiftool_json(exiftool, DATE_KEYS, [p])
        if arr and isinstance(arr, list) and arr:
            obj = arr[0]
            for k in DATE_KEYS:
                vals = _to_str_values(obj.get(k))
                for sv in vals:
                    dt = parse_date_string(sv)
                    if dt:
                        dt = _normalize_to_local(dt)
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
def sidecars_for(p: Path, base_dir: Path) -> Iterable[Path]:
    stem = p.with_suffix("").name
    for ext in SIDECAR_EXT:
        cand = base_dir / f"{stem}{ext}"
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

def find_duplicate_groups(base: Path, recursive: bool):
    size_map: Dict[int, List[Path]] = defaultdict(list)
    total_files = 0
    total_bytes = 0
    for p in walk_files(base, recursive):
        try:
            st = p.stat()
        except Exception:
            continue
        size_map[st.st_size].append(p)
        total_files += 1
        total_bytes += st.st_size
        if total_files % 1000 == 0:
            print(f"  … indicizzati {total_files} file")

    partial_groups: Dict[Tuple[int, str], List[Path]] = defaultdict(list)
    for size, group in size_map.items():
        if len(group) < 2:
            continue
        for p in group:
            try:
                ph = file_sha1_head(p, PARTIAL_HASH_BYTES)
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

def scan_duplicates(base: Path, recursive: bool):
    print(f"Inizio scansione duplicati in: {base}  (ricorsivo: {'Sì' if recursive else 'No'})")
    dup_groups, total_files, total_bytes = find_duplicate_groups(base, recursive)
    groups_count = len(dup_groups)
    potential_savings = 0

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
        f.write(f"=== RIEPILOGO ===\nGruppi duplicati: {groups_count}\nPotenziale spazio recuperabile: {human_size(potential_savings)}\n")

    print(f"Scansione completata. Gruppi duplicati: {groups_count}.")
    print(f"Report scritto in: {base / DUP_SCAN_REPORT}")
    return dup_groups

def choose_keeper(paths: List[Path]) -> Path:
    try:
        return min(paths, key=lambda p: p.stat().st_mtime)  # tieni il più vecchio per mtime
    except Exception:
        return min(paths, key=lambda p: str(p))

def consolidate_duplicates(base: Path, dup_groups: Dict[str, List[Path]], mode: str = "quarantine"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S$")
    quarantine_root = base / f"_QuarantenaDuplicati_{timestamp}"
    if mode == "quarantine":
        quarantine_root.mkdir(parents=True, exist_ok=True)
        print(f"Cartella QUARANTENA: {quarantine_root}")

    actions = []
    groups = sorted(dup_groups.items(), key=lambda x: -len(x[1]))
    for i, (h, paths) in enumerate(groups, start=1):
        if len(paths) < 2:
            continue
        keeper = choose_keeper(paths)
        print(f"[{i}/{len(groups)}] Keeper: {keeper}")
        for p in paths:
            if p == keeper:
                continue
            if mode == "quarantine":
                target_dir = quarantine_root / h
                target_dir.mkdir(parents=True, exist_ok=True)
                dest = target_dir / p.name
                if dest.exists():
                    stem, ext = dest.stem, dest.suffix
                    n = 2
                    while (target_dir / f"{stem}_{n}{ext}").exists():
                        n += 1
                    dest = target_dir / f"{stem}_{n}{ext}"
                shutil.move(str(p), str(dest))
                actions.append(("MOVED", str(p), str(dest)))
                print(f"    spostato -> {dest}")
            elif mode == "delete":
                try:
                    os.remove(p)
                    actions.append(("DELETED", str(p)))
                    print(f"    eliminato -> {p}")
                except Exception as e:
                    print(f"    [ERRORE] Eliminando {p}: {e}")

    with (base / DUP_ACTION_LOG).open("a", encoding="utf-8") as f:
        f.write(f"Azione duplicati — {datetime.now().isoformat()}  mode={mode}\n")
        for a in actions:
            f.write(" ".join(a) + "\n")
        f.write("\n")

    if mode == "quarantine":
        print("Operazione completata: COPIE duplicate spostate in QUARANTENA. Verifica e poi, se vuoi, elimina la cartella.")
    else:
        print("Operazione completata: COPIE duplicate eliminate definitivamente.")

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

# ----------------------------
# main
# ----------------------------
def main():
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
        dup_groups, _, _ = find_duplicate_groups(base, recursive)
        if dup_groups:
            _ = scan_duplicates(base, recursive)
            fix = input("Vuoi RISOLVERE i duplicati adesso lasciandone uno solo? [s/N]: ").strip().lower()
            if fix == "s":
                mode = input("Come procedo? [1 = sposta in QUARANTENA (consigliato), 2 = ELIMINA definitivamente]: ").strip()
                mode = "delete" if mode == "2" else "quarantine"
                print("ATTENZIONE: l'operazione è potenzialmente distruttiva. Continua? [s/N]: ", end="")
                if input().strip().lower() == "s":
                    consolidate_duplicates(base, dup_groups, mode=mode)
                else:
                    print("Annullato su richiesta utente.")
        else:
            print("Nessun duplicato trovato.")
        print("\nFINE scansione/risoluzione duplicati. Esco.")
        return

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

    # Lettura EXIF in batch per i candidati
    exif_map = exif_dates_batch(exiftool, candidates) if exiftool else {}

    moved = 0
    skipped_dup = 0
    conflicts = 0

    try:
        total = len(candidates)
        for idx, src in enumerate(candidates, start=1):
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
                    continue

                if src_hash == dst_hash:
                    print(f"[DUP] {src.name} identico già in {year}/{month}. Segnato in {REPORT_FILE}.")
                    append_report_line(base, src, dest_file, dry_run)
                    skipped_dup += 1
                    continue
                else:
                    print(f"[CONFLITTO] {src.name} esiste già in {year}/{month} ma è diverso. Non sposto.")
                    conflicts += 1
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
                    continue

                for sc in sidecars_for(dest_file, base):
                    sc_dest = dest_dir / sc.name
                    if sc_dest.exists():
                        try:
                            if file_sha1(sc) == file_sha1(sc_dest):
                                print(f"         (sidecar) {sc.name} già presente (identico). Salto.")
                            else:
                                print(f"         (sidecar) CONFLITTO {sc.name} già presente ma diverso. Non sposto.")
                                conflicts += 1
                        except Exception:
                            conflicts += 1
                    else:
                        try:
                            shutil.move(str(sc), str(sc_dest))
                            print(f"         (sidecar) spostato {sc.name}")
                        except Exception as e:
                            print(f"         (sidecar) ERRORE spostando {sc.name}: {e}")
    except KeyboardInterrupt:
        print("\n[INTERRUZIONE] Esecuzione interrotta dall'utente. Riepilogo parziale sotto.")

    print("\n--- Riepilogo ---")
    print(f"Spostati   : {moved}")
    print(f"Duplicati  : {skipped_dup} (vedi {REPORT_FILE} se > 0)")
    print(f"Conflitti  : {conflicts}")

if __name__ == "__main__":
    sys.exit(main())
