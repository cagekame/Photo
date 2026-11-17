"""Finestra iniziale: scelta della cartella e di cosa fare, tutto in un'unica schermata.

Sostituisce le domande nel terminale, che si rompevano se nel terminale finiva altro testo
(es. premendo di nuovo ▶ in VS Code mentre lo script aspettava una risposta).
"""
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Optional

from .dialogs import enable_dpi_awareness


@dataclass
class StartOptions:
    base: Path
    action: str               # "duplicates" | "organize"
    recursive: bool = True    # duplicati: includi sottocartelle
    resolve: str = "review"   # duplicati: "review" | "quarantine" | "delete" | "report"
    dry_run: bool = True      # organizza: solo simulazione
    batch_size: Optional[int] = None


class StartWindow:
    def __init__(self, root: tk.Tk, initial_folder: Optional[Path]):
        self.root = root
        self.result: Optional[StartOptions] = None

        self.folder = tk.StringVar(value=str(initial_folder) if initial_folder else "")
        self.action = tk.StringVar(value="duplicates")
        self.recursive = tk.BooleanVar(value=True)
        self.resolve = tk.StringVar(value="review")
        self.dry_run = tk.BooleanVar(value=True)
        self.batch = tk.StringVar(value="")

        self._build()
        for var in (self.folder, self.action):
            var.trace_add("write", lambda *_: self._refresh())
        self._refresh()

    def _build(self):
        root = self.root
        root.title("Organizza foto e video")
        root.resizable(False, False)
        frm = ttk.Frame(root, padding=16)
        frm.pack(fill="both", expand=True)

        # Cartella
        ttk.Label(frm, text="Cartella da elaborare", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        row = ttk.Frame(frm)
        row.grid(row=1, column=0, sticky="we", pady=(4, 14))
        ttk.Entry(row, textvariable=self.folder, width=60).pack(side="left", fill="x", expand=True)
        ttk.Button(row, text="Sfoglia…", command=self._browse).pack(side="left", padx=(6, 0))

        ttk.Label(frm, text="Cosa vuoi fare?", font=("Segoe UI", 10, "bold")).grid(row=2, column=0, sticky="w")

        # Duplicati
        dup = ttk.Frame(frm, padding=(0, 6, 0, 0))
        dup.grid(row=3, column=0, sticky="w")
        ttk.Radiobutton(dup, text="Cerca i DUPLICATI (non sposta nulla finché non confermi)",
                        variable=self.action, value="duplicates").grid(row=0, column=0, columnspan=2, sticky="w")
        self.dup_widgets = [
            ttk.Checkbutton(dup, text="Includi le sottocartelle", variable=self.recursive),
            ttk.Label(dup, text="Dopo la scansione:"),
            ttk.Radiobutton(dup, text="apri la finestra con le anteprime e scelgo io quale tenere (consigliato)",
                            variable=self.resolve, value="review"),
            ttk.Radiobutton(dup, text="automatico: sposta le copie in QUARANTENA",
                            variable=self.resolve, value="quarantine"),
            ttk.Radiobutton(dup, text="automatico: ELIMINA le copie definitivamente",
                            variable=self.resolve, value="delete"),
            ttk.Radiobutton(dup, text="solo report, non toccare nulla",
                            variable=self.resolve, value="report"),
        ]
        for i, w in enumerate(self.dup_widgets, start=1):
            indent = 24 if i <= 2 else 44
            w.grid(row=i, column=0, sticky="w", padx=(indent, 0), pady=1)

        # Organizzazione
        org = ttk.Frame(frm, padding=(0, 12, 0, 0))
        org.grid(row=4, column=0, sticky="w")
        ttk.Radiobutton(org, text="ORGANIZZA i file in cartelle anno/mese (solo la cartella scelta, non le sottocartelle)",
                        variable=self.action, value="organize").grid(row=0, column=0, columnspan=2, sticky="w")
        batch_row = ttk.Frame(org)
        self.org_widgets = [
            ttk.Checkbutton(org, text="Solo PROVA: mostra cosa farebbe senza spostare nulla", variable=self.dry_run),
            batch_row,
        ]
        self.batch_label = ttk.Label(batch_row, text="Limita a")
        self.batch_label.pack(side="left")
        self.batch_entry = ttk.Entry(batch_row, textvariable=self.batch, width=8)
        self.batch_entry.pack(side="left", padx=4)
        self.batch_hint = ttk.Label(batch_row, text="file (vuoto = tutti)")
        self.batch_hint.pack(side="left")
        for i, w in enumerate(self.org_widgets, start=1):
            w.grid(row=i, column=0, sticky="w", padx=(24, 0), pady=1)

        # Pulsanti
        btns = ttk.Frame(frm, padding=(0, 18, 0, 0))
        btns.grid(row=5, column=0, sticky="e")
        ttk.Button(btns, text="Esci", command=self._cancel).pack(side="right")
        self.start_btn = ttk.Button(btns, text="Avvia", command=self._start)
        self.start_btn.pack(side="right", padx=6)

        root.bind("<Return>", lambda e: self._start())
        root.bind("<Escape>", lambda e: self._cancel())
        root.protocol("WM_DELETE_WINDOW", self._cancel)

    def _browse(self):
        current = self.folder.get().strip()
        chosen = filedialog.askdirectory(parent=self.root, title="Scegli la cartella di foto/video da elaborare",
                                         initialdir=current if current and Path(current).is_dir() else str(Path.cwd()),
                                         mustexist=True)
        if chosen:
            self.folder.set(str(Path(chosen)))

    def _refresh(self):
        dup_on = self.action.get() == "duplicates"
        for w in self.dup_widgets:
            w.state(["!disabled"] if dup_on else ["disabled"])
        for w in (self.org_widgets[0], self.batch_label, self.batch_entry, self.batch_hint):
            w.state(["disabled"] if dup_on else ["!disabled"])
        folder = self.folder.get().strip()
        self.start_btn.state(["!disabled"] if folder and Path(folder).is_dir() else ["disabled"])

    def _start(self):
        folder = self.folder.get().strip()
        if not folder or not Path(folder).is_dir():
            messagebox.showwarning("Cartella", "Scegli una cartella esistente.", parent=self.root)
            return
        batch_size = None
        if self.action.get() == "organize":
            b = self.batch.get().strip()
            if b:
                if not b.isdigit() or int(b) <= 0:
                    messagebox.showwarning("Limite file", "Scrivi un numero intero positivo, oppure lascia vuoto.",
                                           parent=self.root)
                    return
                batch_size = int(b)
            if not self.dry_run.get() and not messagebox.askyesno(
                    "Conferma", f"I file in\n{folder}\nverranno SPOSTATI nelle cartelle anno/mese.\n\nProcedo?",
                    parent=self.root):
                return
        self.result = StartOptions(
            base=Path(folder), action=self.action.get(), recursive=self.recursive.get(),
            resolve=self.resolve.get(), dry_run=self.dry_run.get(), batch_size=batch_size)
        self.root.destroy()

    def _cancel(self):
        self.result = None
        self.root.destroy()


def ask_start_options(initial_folder: Optional[Path] = None) -> Optional[StartOptions]:
    """Mostra la finestra iniziale. Ritorna None se l'utente esce."""
    enable_dpi_awareness()
    root = tk.Tk()
    win = StartWindow(root, initial_folder)
    root.lift()
    root.attributes("-topmost", True)
    root.after(300, lambda: root.attributes("-topmost", False))  # in primo piano, ma senza restarci
    root.focus_force()
    root.mainloop()
    return win.result
