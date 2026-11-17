"""Avvio: flag da riga di comando + finestra iniziale. Nessuna domanda nel terminale."""
import argparse
import sys
from pathlib import Path

from .config import settings
from .dialogs import confirm, info
from .duplicates import consolidate_duplicates, scan_duplicates
from .exif import exiftool_version, get_exiftool_path
from .fileutils import human_size
from .organize import organize
from .start_window import StartOptions, ask_start_options


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Organizza foto/video in cartelle YYYY/MM e gestisce i duplicati.")
    ap.add_argument("--cartella", type=Path, default=None,
                    help="Cartella proposta nella finestra iniziale.")
    ap.add_argument("--no-local-tz", action="store_true",
                    help="Non normalizzare le date al fuso locale (già il comportamento predefinito).")
    ap.add_argument("--utc", action="store_true", help="Normalizza le date a UTC.")
    ap.add_argument("--batch-exif", type=int, default=settings.batch_exif_size,
                    help="Dimensione batch lettura EXIF (default 25).")
    ap.add_argument("--prehash-bytes", type=int, default=settings.prehash_bytes,
                    help="Dimensione pre-hash in byte (default 4MB, minimo 1MB).")
    ap.add_argument("--no-shell-fallback", action="store_true", help="Disabilita fallback shell=True (Windows).")
    ap.add_argument("--disable-near-dup", action="store_true",
                    help="Disattiva il rilevamento dei video quasi uguali (richiede ffprobe).")
    ap.add_argument("--qt-utc", action="store_true",
                    help="Usa -api QuickTimeUTC in exiftool (tempi QuickTime trattati come UTC).")
    return ap.parse_args(argv)


def _apply_settings(args):
    if args.no_local_tz:
        settings.normalize_to_local = False
    if args.utc:
        settings.normalize_to_local = False
        settings.normalize_to_utc = True
    settings.batch_exif_size = max(1, args.batch_exif)
    settings.prehash_bytes = max(1024 * 1024, args.prehash_bytes)
    if args.no_shell_fallback:
        settings.use_shell_fallback = False
    if args.qt_utc:
        settings.quicktime_utc = True


def _run_duplicates(opts: StartOptions, exiftool, near_dup: bool):
    base = opts.base
    dup_groups = scan_duplicates(base, opts.recursive, settings.prehash_bytes, enable_near_dup=near_dup)
    if not dup_groups:
        print("Nessun duplicato trovato.")
        info("Duplicati", "Nessun duplicato trovato.")
        return

    copies = sum(len(ps) - 1 for ps in dup_groups.values())
    if opts.resolve == "report":
        info("Duplicati", f"Trovati {len(dup_groups)} gruppi di duplicati ({copies} copie in più).\n\n"
                          f"Report: {base / 'duplicati_scan.txt'}")
        return

    if opts.resolve == "review":
        from . import thumbnails
        from .gui_duplicates import review_duplicates
        if not thumbnails.pillow_available():
            print(f"[ATTENZIONE] Pillow non utilizzabile con questo Python ({sys.executable}):\n"
                  f"             {thumbnails.PILLOW_ERROR}\n"
                  f"             La finestra si apre lo stesso, ma senza anteprime.\n"
                  f"             Per sistemare: \"{sys.executable}\" -m pip install pillow pillow-heif")
        print("Apro la finestra di revisione…")
        result = review_duplicates(base, dup_groups, exiftool)
        if result is None:
            print("Revisione annullata: nessun file modificato.")
            return
        keepers, mode = result
        consolidate_duplicates(base, dup_groups, mode=mode, keepers=keepers)
        return

    # Automatico: quarantena o eliminazione, con conferma
    size = 0
    for ps in dup_groups.values():
        try:
            size += ps[0].stat().st_size * (len(ps) - 1)
        except Exception:
            pass
    delete = opts.resolve == "delete"
    action = "ELIMINATE DEFINITIVAMENTE" if delete else "spostate in quarantena"
    if confirm("Conferma",
               f"Trovati {len(dup_groups)} gruppi di duplicati.\n"
               f"{copies} copie ({human_size(size)}) verranno {action},\n"
               f"tenendo in ogni gruppo la copia già organizzata o la più vecchia.\n\nProcedo?",
               warning=delete):
        consolidate_duplicates(base, dup_groups, mode="delete" if delete else "quarantine")
    else:
        print("Annullato: nessun file modificato.")


def main(app_dir: Path, argv=None) -> int:
    """app_dir: cartella dello script (dove si cercano exiftool e exiftool_path.txt)."""
    args = parse_args(argv)
    _apply_settings(args)

    print("Scegli cartella e operazione nella finestra che si è aperta…")
    opts = ask_start_options(args.cartella.resolve() if args.cartella else None)
    if opts is None:
        print("Uscita senza fare nulla.")
        return 0
    base = opts.base.resolve()
    opts.base = base

    exiftool = get_exiftool_path(app_dir)
    print(f"Cartella di lavoro: {base}")
    if exiftool:
        print(f"exiftool: trovato -> {exiftool} (ver: {exiftool_version(exiftool) or 'sconosciuta'})")
    else:
        print("exiftool: NON trovato, userò mtime come fallback")

    if opts.action == "duplicates":
        _run_duplicates(opts, exiftool, near_dup=not args.disable_near_dup)
    else:
        organize(base, exiftool, opts.dry_run, opts.batch_size)
        info("Organizzazione", "Operazione terminata.\nIl riepilogo è nel terminale.")
    print("\nFINE.")
    return 0
