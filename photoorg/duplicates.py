"""Ricerca dei duplicati e loro risoluzione (quarantena / eliminazione)."""
import json
import os
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import (DUP_ACTION_LOG, DUP_ACTIONS_JSONL, DUP_SCAN_JSONL, DUP_SCAN_NEAR_REPORT,
                     DUP_SCAN_REPORT, QUARANTINE_PREFIX, VIDEO_EXT)
from .fileutils import (append_jsonl, file_sha1, file_sha1_head, human_size,
                        other_media_with_stem, sidecars_for, walk_files)

DupGroups = Dict[str, List[Path]]


# ----------------------------
# Ricerca duplicati identici
# ----------------------------

def find_duplicate_groups(base: Path, recursive: bool, prehash_bytes: int) -> Tuple[DupGroups, int, int]:
    """Dimensione → hash dei primi byte → SHA1 completo."""
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
            print(f"  … indicizzati {total_files} file (≈{total_files / elapsed:.1f} file/s)")

    partial_groups: Dict[Tuple[int, str], List[Path]] = defaultdict(list)
    for size, group in size_map.items():
        if len(group) < 2:
            continue
        for p in group:
            try:
                partial_groups[(size, file_sha1_head(p, prehash_bytes))].append(p)
            except Exception:
                continue

    full_groups: DupGroups = defaultdict(list)
    for group in partial_groups.values():
        if len(group) < 2:
            continue
        for p in group:
            try:
                full_groups[file_sha1(p)].append(p)
            except Exception:
                continue

    dup_groups = {h: lst for h, lst in full_groups.items() if len(lst) > 1}
    return dup_groups, total_files, total_bytes


def sorted_groups(dup_groups: DupGroups) -> List[Tuple[str, List[Path]]]:
    """Ordine usato ovunque (report, finestra, risoluzione): gruppi più numerosi prima."""
    return sorted(dup_groups.items(), key=lambda x: (-len(x[1]), x[0]))


# ----------------------------
# Near-duplicate video (ffprobe)
# ----------------------------

def detect_ffprobe() -> Optional[str]:
    return shutil.which("ffprobe")


def video_fingerprint(ffprobe: str, p: Path) -> Optional[Tuple[int, int, str, float, float]]:
    """(width, height, codec, durata arrotondata a 0.5s, fps arrotondati a 0.01) del primo stream video."""
    try:
        cmd = [ffprobe, "-v", "error", "-select_streams", "v:0",
               "-show_entries", "stream=width,height,codec_name,avg_frame_rate,duration",
               "-of", "json", str(p)]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            return None
        streams = json.loads(r.stdout).get("streams") or []
        if not streams:
            return None
        s0 = streams[0]
        dur = s0.get("duration")
        if dur is None:
            return None
        w = int(float(s0.get("width", 0) or 0))
        h = int(float(s0.get("height", 0) or 0))
        codec = str(s0.get("codec_name") or "?")
        durf = round(float(dur) * 2) / 2.0
        fps = 0.0
        num, _, den = str(s0.get("avg_frame_rate") or "0/1").partition("/")
        try:
            if float(den or 1):
                fps = round(float(num) / float(den or 1), 2)
        except ValueError:
            pass
        return (w, h, codec, durf, fps)
    except Exception:
        return None


def scan_near_duplicates(base: Path, recursive: bool) -> Optional[int]:
    """Scrive DUP_SCAN_NEAR_REPORT. Ritorna il numero di gruppi, o None se ffprobe manca."""
    ffprobe = detect_ffprobe()
    if not ffprobe:
        print("[NEAR-DUP] ffprobe non trovato: salto la rilevazione duplicati quasi uguali.")
        return None
    fp_map: Dict[tuple, List[Path]] = defaultdict(list)
    for p in walk_files(base, recursive):
        if p.suffix.lower() in VIDEO_EXT:
            fp = video_fingerprint(ffprobe, p)
            if fp:
                fp_map[fp].append(p)

    near_groups = []
    for fp, lst in fp_map.items():
        if len(lst) < 2:
            continue
        sha_map: Dict[str, List[Path]] = defaultdict(list)
        for p in lst:
            try:
                sha_map[file_sha1(p)].append(p)
            except Exception:
                continue
        if len(sha_map) > 1:
            near_groups.append((fp, sha_map))

    if near_groups:
        print(f"[NEAR-DUP] Trovati {len(near_groups)} gruppi di probabili duplicati ricodificati.")
        with (base / DUP_SCAN_NEAR_REPORT).open("w", encoding="utf-8") as nf:
            for idx, ((w, h, codec, dur, fps), sha_map) in enumerate(near_groups, start=1):
                nf.write(f"[{idx}] {w}x{h} {codec} {fps}fps dur≈{dur}s\n")
                for hh, plist in sha_map.items():
                    nf.write(f"  SHA1={hh} (n={len(plist)})\n")
                    for p in plist:
                        nf.write(f"    - {p}\n")
                nf.write("\n")
    return len(near_groups)


# ----------------------------
# Scansione con report
# ----------------------------

def scan_duplicates(base: Path, recursive: bool, prehash_bytes: int, enable_near_dup: bool = True) -> DupGroups:
    print(f"Inizio scansione duplicati in: {base}  (ricorsivo: {'Sì' if recursive else 'No'})")
    dup_groups, total_files, total_bytes = find_duplicate_groups(base, recursive, prehash_bytes)
    potential_savings = 0

    try:
        (base / DUP_SCAN_JSONL).unlink(missing_ok=True)
    except Exception:
        pass

    with (base / DUP_SCAN_REPORT).open("w", encoding="utf-8") as f:
        f.write(f"Report duplicati — generato: {datetime.now().isoformat()}\n")
        f.write(f"Cartella base: {base}\nRicorsivo: {'Sì' if recursive else 'No'}\n")
        f.write(f"File indicizzati: {total_files}  (totale dati: {human_size(total_bytes)})\n\n")
        for i, (h, paths) in enumerate(sorted_groups(dup_groups), start=1):
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
            append_jsonl(base / DUP_SCAN_JSONL, {
                "group_index": i, "sha1": h, "count": len(paths), "size": sz,
                "potential_saving_bytes": save, "paths": [str(x) for x in paths],
            })
        f.write(f"=== RIEPILOGO ===\nGruppi duplicati: {len(dup_groups)}\n"
                f"Potenziale spazio recuperabile: {human_size(potential_savings)}\n")

    near = scan_near_duplicates(base, recursive) if enable_near_dup else None

    print(f"Scansione completata. Gruppi duplicati: {len(dup_groups)}  "
          f"(spazio recuperabile: {human_size(potential_savings)}).")
    if near is not None:
        print(f"Gruppi near-duplicate: {near} (vedi {DUP_SCAN_NEAR_REPORT})")
    print(f"Report scritto in: {base / DUP_SCAN_REPORT}")
    return dup_groups


# ----------------------------
# Scelta della copia da tenere
# ----------------------------
# I file di un gruppo sono identici byte per byte (stessi metadati EXIF), quindi si preferisce:
#   1) la copia già organizzata in una cartella YYYY/MM
#   2) la copia con mtime più vecchio
#   3) il percorso più corto, poi l'ordine alfabetico

def in_dated_folder(p: Path) -> bool:
    month = p.parent.name
    year = p.parent.parent.name
    return (len(year) == 4 and year.isdigit()
            and len(month) == 2 and month.isdigit() and 1 <= int(month) <= 12)


def keeper_key(p: Path) -> Tuple[int, float, int, str]:
    try:
        ts = p.stat().st_mtime
    except Exception:
        ts = float("inf")
    return (0 if in_dated_folder(p) else 1, ts, len(str(p)), str(p))


def choose_keeper(paths: List[Path]) -> Path:
    return min(paths, key=keeper_key)


# ----------------------------
# Risoluzione
# ----------------------------

def _unique_dest(target_dir: Path, name: str) -> Path:
    dest = target_dir / name
    if not dest.exists():
        return dest
    stem, ext = dest.stem, dest.suffix
    n = 2
    while (target_dir / f"{stem}_{n}{ext}").exists():
        n += 1
    return target_dir / f"{stem}_{n}{ext}"


def consolidate_duplicates(base: Path, dup_groups: DupGroups, mode: str,
                           keepers: Optional[Dict[str, Optional[Path]]] = None):
    """Tiene una copia per gruppo e sposta in quarantena (mode="quarantine") o elimina (mode="delete") le altre.

    keepers: scelte fatte dall'utente, {sha1: copia da tenere}. None come valore = gruppo da non toccare.
             Per i gruppi assenti si usa choose_keeper().
    """
    keepers = keepers or {}
    quarantine_root = base / f"{QUARANTINE_PREFIX}{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Piano completo prima di toccare qualsiasi file, così sappiamo quali file spariranno
    plan = []
    for h, paths in sorted_groups(dup_groups):
        if len(paths) < 2:
            continue
        if h in keepers:
            if keepers[h] is None:
                continue  # l'utente ha scelto di lasciare il gruppo com'è
            keeper = keepers[h]
        else:
            keeper = choose_keeper(paths)
        plan.append((h, keeper, paths))
    removing = {p for _, keeper, paths in plan for p in paths if p != keeper}

    if not plan:
        print("Nessun gruppo da risolvere.")
        return
    if mode == "quarantine":
        quarantine_root.mkdir(parents=True, exist_ok=True)
        print(f"Cartella QUARANTENA: {quarantine_root}")
    try:
        (base / DUP_ACTIONS_JSONL).unlink(missing_ok=True)
    except Exception:
        pass

    actions = []
    for i, (h, keeper, paths) in enumerate(plan, start=1):
        print(f"[{i}/{len(plan)}] Keeper: {keeper}")
        # Sicurezza: mai rimuovere copie se il keeper non c'è più
        try:
            keeper_size = keeper.stat().st_size
        except Exception:
            print("    [SALTO] Keeper non più presente, gruppo ignorato.")
            continue
        for p in paths:
            if p == keeper:
                continue
            try:
                if not p.is_file() or p.stat().st_size != keeper_size:
                    print(f"    [SALTO] {p} mancante o modificato dopo la scansione.")
                    continue
            except Exception:
                continue
            related = [p]
            for sc in sidecars_for(p):
                if other_media_with_stem(sc, removing):
                    print(f"    (sidecar) {sc.name} condiviso con un altro file che resta: non lo tocco.")
                else:
                    related.append(sc)

            for item in related:
                try:
                    if mode == "quarantine":
                        target_dir = quarantine_root / h
                        target_dir.mkdir(parents=True, exist_ok=True)
                        dest = _unique_dest(target_dir, item.name)
                        shutil.move(str(item), str(dest))
                        actions.append(("MOVED", str(item), str(dest)))
                        print(f"    spostato -> {dest}")
                        append_jsonl(base / DUP_ACTIONS_JSONL, {
                            "action": "move", "source": str(item), "dest": str(dest), "group_sha1": h})
                    elif mode == "delete":
                        os.remove(item)
                        actions.append(("DELETED", str(item)))
                        print(f"    eliminato -> {item}")
                        append_jsonl(base / DUP_ACTIONS_JSONL, {
                            "action": "delete", "source": str(item), "group_sha1": h})
                except Exception as e:
                    print(f"    [ERRORE] {item}: {e}")

    with (base / DUP_ACTION_LOG).open("a", encoding="utf-8") as f:
        f.write(f"Azione duplicati — {datetime.now().isoformat()}  mode={mode}\n")
        for a in actions:
            f.write(" ".join(a) + "\n")
        f.write("\n")

    if mode == "quarantine":
        print("Operazione completata: copie duplicate spostate in QUARANTENA (con sidecar). "
              "Verifica e poi, se vuoi, elimina la cartella.")
    else:
        print("Operazione completata: copie duplicate eliminate definitivamente (con sidecar).")
