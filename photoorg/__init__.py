"""Organizzatore di foto/video con gestione dei duplicati.

Moduli:
    config      costanti (estensioni, nomi file) e impostazioni modificabili da CLI
    exif        exiftool: ricerca, esecuzione robusta, lettura date, estrazione anteprime
    fileutils   hash, scansione cartelle, sidecar, utilità varie
    duplicates  ricerca duplicati, near-duplicate video, scelta keeper, quarantena/eliminazione
    organize    spostamento in cartelle YYYY/MM con checkpoint
    thumbnails  generazione miniature (Pillow, pillow-heif, anteprime RAW, frame video)
    gui_duplicates  finestra tkinter per scegliere quale copia tenere
    cli         argomenti da riga di comando e menu interattivo
"""
