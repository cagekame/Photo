"""Piccole finestre di dialogo indipendenti (conferma, avviso, scelta file) e impostazioni tkinter condivise."""
import os
import tkinter as tk
from tkinter import filedialog, messagebox
from typing import Optional

_dpi_done = False


def enable_dpi_awareness():
    """Testo nitido sugli schermi ad alta risoluzione (Windows). Va chiamata prima di creare finestre."""
    global _dpi_done
    if _dpi_done or os.name != "nt":
        return
    _dpi_done = True
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass


def _hidden_root() -> tk.Tk:
    enable_dpi_awareness()
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)  # evita che la finestra resti dietro al terminale
    return root


def confirm(title: str, message: str, warning: bool = False) -> bool:
    root = _hidden_root()
    try:
        return messagebox.askyesno(title, message, icon="warning" if warning else "question", parent=root)
    finally:
        root.destroy()


def info(title: str, message: str):
    root = _hidden_root()
    try:
        messagebox.showinfo(title, message, parent=root)
    finally:
        root.destroy()


def choose_file(title: str, filetypes) -> Optional[str]:
    root = _hidden_root()
    try:
        return filedialog.askopenfilename(parent=root, title=title, filetypes=filetypes) or None
    finally:
        root.destroy()
