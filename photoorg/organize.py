"""Spostamento di foto/video nelle cartelle YYYY/MM in base alla data di scatto."""
import json
import shutil
from heapq import nsmallest
from pathlib import Path
from typing import Optional, Set

from .config import CHECKPOINT_FILE, REPORT_FILE, settings
from .exif import exif_dates_batch, get_taken_datetime
from .fileutils import ensure_dir, file_sha1, iter_candidates_fast, other_media_with_stem, sidecars_for


# ----------------------------
# Checkpoint
# ----------------------------
# Contiene i file già trattati in un'esecuzione interrotta (duplicati e conflitti restano nella
# cartella di partenza: senza checkpoint verrebbero ri-hashati e ri-segnalati).
# I file spostati spariscono da soli dalla lista dei candidati.
# Non viene scritto in dry-run e viene cancellato quando l'esecuzione arriva in fondo.

def load_checkpoint(base: Path) -> Set[str]:
    cp = base / CHECKPOINT_FILE
    if not cp.exists():
        return set()
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        return set(data.get("processed", []))
    except Exception:
        return set()


def save_checkpoint(base: Path, processed: Set[str]):
    try:
        (base / CHECKPOINT_FILE).write_text(
            json.dumps({"processed": sorted(processed)}, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def clear_checkpoint(base: Path):
    try:
        (base / CHECKPOINT_FILE).unlink(missing_ok=True)
    except Exception:
        pass


# ----------------------------
# Organizzazione
# ----------------------------

def _append_report_line(base: Path, src: Path, dst: Path):
    with (base / REPORT_FILE).open("a", encoding="utf-8") as f:
        f.write(f"DUPLICATO: '{src.name}' uguale a '{dst.name}' in {dst.parent}\n")


def _move_sidecars(src: Path, dest_dir: Path, dry_run: bool) -> int:
    """Sposta i sidecar rimasti accanto a src. Se un altro file media con lo stesso nome
    è ancora nella cartella di partenza (es. RAW+JPG), il sidecar aspetta e partirà con quello.
    Ritorna il numero di conflitti."""
    conflicts = 0
    for sc in sidecars_for(src):
        if other_media_with_stem(sc, exclude={src}):
            continue
        sc_dest = dest_dir / sc.name
        if dry_run:
            print(f"         (sidecar) sposterei {sc.name}")
            continue
        if sc_dest.exists():
            try:
                if file_sha1(sc) == file_sha1(sc_dest):
                    print(f"         (sidecar) {sc.name} già presente (identico). Salto.")
                else:
                    print(f"         (sidecar) CONFLITTO {sc.name} già presente ma diverso. Non sposto.")
                    conflicts += 1
            except Exception:
                conflicts += 1
            continue
        try:
            shutil.move(str(sc), str(sc_dest))
            print(f"         (sidecar) spostato {sc.name}")
        except Exception as e:
            print(f"         (sidecar) ERRORE spostando {sc.name}: {e}")
    return conflicts


def organize(base: Path, exiftool: Optional[str], dry_run: bool, batch_size: Optional[int]):
    print("Modalità:", "DRY-RUN (simulazione, non sposto nulla)" if dry_run else "NORMALE (sposto i file)")

    processed = set() if dry_run else load_checkpoint(base)
    if processed:
        print(f"Checkpoint trovato: {len(processed)} file già esaminati in un'esecuzione interrotta verranno saltati.")

    print("Preparazione elenco files…")
    pending = (t for t in iter_candidates_fast(base) if str(t[1]) not in processed)
    if batch_size:
        candidates = [p for _, p in nsmallest(batch_size, pending, key=lambda t: t[0])]
        print(f"Limiterò il lavoro a {len(candidates)} file in questo batch (selezione rapida).")
    else:
        candidates = [p for _, p in sorted(pending, key=lambda t: t[0])]
        print(f"Trovati {len(candidates)} file candidati. Elenco preparato.")

    exif_map = exif_dates_batch(exiftool, candidates, settings.batch_exif_size) if exiftool else {}

    moved = skipped_dup = conflicts = 0
    completed = False
    try:
        total = len(candidates)
        for idx, src in enumerate(candidates, start=1):
            taken = get_taken_datetime(src, exiftool, premap=exif_map)
            year, month = f"{taken.year:04d}", f"{taken.month:02d}"
            dest_dir = base / year / month
            ensure_dir(dest_dir, dry_run)
            dest_file = dest_dir / src.name
            print(f"[{idx}/{total}] {src.name}")

            if dest_file.exists():
                try:
                    same = file_sha1(src) == file_sha1(dest_file)
                except Exception as e:
                    print(f"[ERRORE] Hash su {src.name}: {e}")
                    conflicts += 1
                    same = None
                if same:
                    print(f"[DUP] {src.name} identico già in {year}/{month}. Segnato in {REPORT_FILE}.")
                    if not dry_run:
                        _append_report_line(base, src, dest_file)
                    skipped_dup += 1
                elif same is False:
                    print(f"[CONFLITTO] {src.name} esiste già in {year}/{month} ma è diverso. Non sposto.")
                    conflicts += 1
            elif dry_run:
                print(f"[SIMULA] Sposterei {src.name} -> {year}/{month}/")
                _move_sidecars(src, dest_dir, dry_run=True)
            else:
                try:
                    shutil.move(str(src), str(dest_file))
                    moved += 1
                    print(f"[SPOSTATO] {dest_file.relative_to(base)}")
                    conflicts += _move_sidecars(src, dest_dir, dry_run=False)
                except Exception as e:
                    print(f"[ERRORE] Spostando {src.name}: {e}")

            if not dry_run:
                processed.add(str(src))
                save_checkpoint(base, processed)
        completed = True
    except KeyboardInterrupt:
        print("\n[INTERRUZIONE] Esecuzione interrotta dall'utente. Rilancia lo script per riprendere.")

    # In modalità batch il checkpoint resta finché non sono stati esaminati tutti i file
    if completed and not dry_run and (not batch_size or len(candidates) < batch_size):
        clear_checkpoint(base)

    print("\n--- Riepilogo ---")
    print(f"Spostati   : {moved}")
    print(f"Duplicati  : {skipped_dup} (vedi {REPORT_FILE} se > 0)")
    print(f"Conflitti  : {conflicts}")
