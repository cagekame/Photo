#!/usr/bin/env python3
"""Organizza foto/video in cartelle YYYY/MM e gestisce i duplicati.

Avvio:  python organize_photos.py [opzioni]     (python organize_photos.py --help per l'elenco)
Il codice vero e proprio è nel pacchetto photoorg/.
"""
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))

from photoorg.cli import main  # noqa: E402

if __name__ == "__main__":
    try:
        sys.exit(main(APP_DIR))
    except KeyboardInterrupt:
        print("\nInterrotto.")
        sys.exit(130)
    except Exception as e:
        print(f"[FATALE] {e}")
        sys.exit(1)
