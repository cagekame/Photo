"""Finestra tkinter per rivedere i gruppi di duplicati e scegliere quale copia tenere.

Uso:
    result = review_duplicates(base, dup_groups, exiftool)
    if result:
        keepers, mode = result   # keepers: {sha1: Path da tenere | None = non toccare}

Scorciatoie: ←/→ gruppo precedente/successivo, 1-9 sceglie la copia, 0 lascia il gruppo com'è,
doppio clic su un'anteprima apre il file.
"""
import os
import queue
import subprocess
import sys
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Dict, List, Optional, Tuple

try:
    from PIL import ImageTk
except ImportError:  # senza Pillow make_thumbnail ritorna sempre None
    ImageTk = None

from . import thumbnails
from .dialogs import enable_dpi_awareness
from .duplicates import DupGroups, choose_keeper, in_dated_folder, sorted_groups
from .fileutils import human_size

THUMB_SIZE = 260
COLUMNS = 3
SKIP = -1

# Colori
BG = "#f3f3f3"
CARD_BG = "#ffffff"
THUMB_BG = "#e4e4e4"
KEEP = "#2e7d32"
REMOVE = "#c62828"
NEUTRAL = "#9e9e9e"
MUTED = "#666666"

Result = Tuple[Dict[str, Optional[Path]], str]


def _file_size(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return 0


def _mtime_text(p: Path) -> str:
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "?"


def _open_file(p: Path):
    try:
        if os.name == "nt":
            os.startfile(str(p))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(p)])
        else:
            subprocess.Popen(["xdg-open", str(p)])
    except Exception as e:
        messagebox.showerror("Apri file", f"Impossibile aprire {p}:\n{e}")


class DuplicateReviewer:
    def __init__(self, root: tk.Tk, base: Path, dup_groups: DupGroups, exiftool: Optional[str]):
        self.root = root
        self.base = base
        self.exiftool = exiftool
        self.groups: List[Tuple[str, List[Path]]] = [(h, sorted(ps, key=str)) for h, ps in sorted_groups(dup_groups)]
        self.sizes = {h: _file_size(ps[0]) for h, ps in self.groups}
        # Proposta iniziale: la stessa scelta automatica della modalità non grafica
        self.choices: Dict[str, Optional[Path]] = {h: choose_keeper(ps) for h, ps in self.groups}
        self.idx = 0
        self.result: Optional[Result] = None
        self._closed = False

        self.keep_var = tk.IntVar(value=0)
        self.mode_var = tk.StringVar(value="quarantine")

        # Miniature: generate in thread separati (PIL), convertite in PhotoImage nel thread di tkinter
        self.thumb_cache: Dict[Path, object] = {}
        self.pending: set = set()
        self.thumb_labels: Dict[Path, tk.Label] = {}
        self.cards: List[Tuple[tk.Frame, tk.Label]] = []
        self.queue: "queue.Queue" = queue.Queue()
        self.pool = ThreadPoolExecutor(max_workers=4)

        self._build()
        self._show_group(0)
        self.root.after(50, self._poll_thumbs)

    # ---------- costruzione interfaccia ----------

    def _build(self):
        root = self.root
        root.title(f"Revisione duplicati — {self.base}")
        root.geometry("1250x820")
        root.minsize(900, 600)
        root.configure(background=BG)
        style = ttk.Style(root)
        style.configure("Header.TLabel", font=("Segoe UI", 13, "bold"))
        style.configure("Muted.TLabel", foreground=MUTED)

        # Barra inferiore (creata per prima così resta sempre visibile)
        bar = ttk.Frame(root, padding=8)
        bar.pack(side="bottom", fill="x")
        ttk.Button(bar, text="◀ Precedente", command=self.prev).pack(side="left")
        ttk.Button(bar, text="Successivo ▶", command=self.next).pack(side="left", padx=(6, 20))
        ttk.Label(bar, text="Copie scartate:").pack(side="left")
        ttk.Radiobutton(bar, text="Quarantena (consigliato)", variable=self.mode_var, value="quarantine",
                        command=self._on_mode).pack(side="left", padx=4)
        ttk.Radiobutton(bar, text="Elimina definitivamente", variable=self.mode_var, value="delete",
                        command=self._on_mode).pack(side="left")
        ttk.Button(bar, text="Annulla", command=self.cancel).pack(side="right")
        ttk.Button(bar, text="Applica scelte…", command=self.apply).pack(side="right", padx=6)
        self.summary = ttk.Label(bar, font=("Segoe UI", 9, "bold"))
        self.summary.pack(side="right", padx=12)

        paned = ttk.PanedWindow(root, orient="horizontal")
        paned.pack(fill="both", expand=True)

        # Elenco gruppi
        left = ttk.Frame(paned, padding=6)
        ttk.Label(left, text="Gruppi di duplicati", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        lb_frame = ttk.Frame(left)
        lb_frame.pack(fill="both", expand=True, pady=(4, 0))
        self.listbox = tk.Listbox(lb_frame, width=38, activestyle="none", exportselection=False,
                                  font=("Segoe UI", 9))
        sb = ttk.Scrollbar(lb_frame, orient="vertical", command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=sb.set)
        self.listbox.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        for i in range(len(self.groups)):
            self.listbox.insert("end", self._list_text(i))
        self.listbox.bind("<<ListboxSelect>>", self._on_list_select)
        paned.add(left, weight=0)

        # Area gruppo corrente
        right = ttk.Frame(paned, padding=(8, 6, 6, 0))
        paned.add(right, weight=1)
        self.header = ttk.Label(right, style="Header.TLabel")
        self.header.pack(anchor="w")
        self.subheader = ttk.Label(right, style="Muted.TLabel")
        self.subheader.pack(anchor="w")
        ttk.Radiobutton(right, text="Non toccare questo gruppo (tieni tutte le copie)   [0]",
                        variable=self.keep_var, value=SKIP, command=self._on_choice).pack(anchor="w", pady=(6, 0))

        area = ttk.Frame(right)
        area.pack(fill="both", expand=True, pady=6)
        self.canvas = tk.Canvas(area, highlightthickness=0, background=BG)
        vsb = ttk.Scrollbar(area, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=vsb.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self.cards_frame = tk.Frame(self.canvas, background=BG)
        self.canvas.create_window((0, 0), window=self.cards_frame, anchor="nw")
        self.cards_frame.bind("<Configure>",
                              lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.bind_all("<MouseWheel>", self._on_wheel)

        # Scorciatoie
        root.bind("<Left>", lambda e: self.prev())
        root.bind("<Right>", lambda e: self.next())
        root.bind("0", lambda e: self._select(SKIP))
        for n in range(1, 10):
            root.bind(str(n), lambda e, n=n: self._select(n - 1))
        root.protocol("WM_DELETE_WINDOW", self.cancel)

    def _on_wheel(self, event):
        # Il wheel sull'elenco gruppi scorre l'elenco, altrove scorre le anteprime
        if event.widget is self.listbox:
            return
        self.canvas.yview_scroll(int(-event.delta / 120), "units")

    # ---------- elenco gruppi ----------

    def _list_text(self, i: int) -> str:
        h, paths = self.groups[i]
        status = "  — non toccare" if self.choices[h] is None else ""
        return f"{i + 1:>4}. {paths[0].name}  ×{len(paths)}  ({human_size(self.sizes[h])}){status}"

    def _refresh_list_row(self, i: int):
        self.listbox.delete(i)
        self.listbox.insert(i, self._list_text(i))
        h = self.groups[i][0]
        self.listbox.itemconfig(i, foreground=NEUTRAL if self.choices[h] is None else "black")
        if i == self.idx:
            self.listbox.selection_set(i)

    def _on_list_select(self, _event):
        sel = self.listbox.curselection()
        if sel and sel[0] != self.idx:
            self._show_group(sel[0])

    # ---------- gruppo corrente ----------

    def _show_group(self, i: int):
        self.idx = i
        h, paths = self.groups[i]
        for w in self.cards_frame.winfo_children():
            w.destroy()
        self.thumb_labels.clear()
        self.cards = []

        keeper = self.choices[h]
        self.keep_var.set(SKIP if keeper is None else paths.index(keeper))
        self.header.configure(text=f"Gruppo {i + 1} di {len(self.groups)}")
        self.subheader.configure(
            text=f"{len(paths)} copie identiche · {human_size(self.sizes[h])} ciascuna · SHA1 {h[:12]}…   "
                 f"(doppio clic su un'anteprima per aprire il file)")

        for j, p in enumerate(paths):
            card = self._make_card(j, p)
            card.grid(row=j // COLUMNS, column=j % COLUMNS, padx=8, pady=8, sticky="n")

        self._refresh_cards()
        self._request_thumbs(paths)
        if i + 1 < len(self.groups):  # precarica il gruppo successivo
            self._request_thumbs(self.groups[i + 1][1])

        self.listbox.selection_clear(0, "end")
        self.listbox.selection_set(i)
        self.listbox.see(i)
        self.canvas.yview_moveto(0)
        self._update_summary()

    def _make_card(self, j: int, p: Path) -> tk.Frame:
        card = tk.Frame(self.cards_frame, background=CARD_BG, highlightthickness=3, padx=8, pady=8)
        box = tk.Frame(card, width=THUMB_SIZE, height=THUMB_SIZE, background=THUMB_BG)
        box.pack_propagate(False)
        box.pack()
        thumb = tk.Label(box, text="Caricamento…", background=THUMB_BG, foreground=MUTED, justify="center")
        thumb.pack(fill="both", expand=True)
        self.thumb_labels[p] = thumb
        if p in self.thumb_cache:
            self._apply_thumb(thumb, p)

        for w in (box, thumb):
            w.bind("<Button-1>", lambda e, j=j: self._select(j))
            w.bind("<Double-Button-1>", lambda e, p=p: _open_file(p))

        status = tk.Label(card, font=("Segoe UI", 10, "bold"), background=CARD_BG)
        status.pack(anchor="w", pady=(6, 0))
        ttk.Radiobutton(card, text=f"[{j + 1}] Tieni questa", variable=self.keep_var, value=j,
                        command=self._on_choice).pack(anchor="w")
        tk.Label(card, text=p.name, font=("Segoe UI", 9, "bold"), background=CARD_BG,
                 wraplength=THUMB_SIZE, justify="left").pack(anchor="w")
        try:
            folder = str(p.parent.relative_to(self.base))
            if folder == ".":
                folder = "(cartella principale)"
        except ValueError:
            folder = str(p.parent)
        tk.Label(card, text=f"📁 {folder}", background=CARD_BG, foreground=MUTED,
                 wraplength=THUMB_SIZE, justify="left").pack(anchor="w")
        tk.Label(card, text=f"Modificato: {_mtime_text(p)}", background=CARD_BG,
                 foreground=MUTED).pack(anchor="w")
        if in_dated_folder(p):
            tk.Label(card, text="✓ già in una cartella anno/mese", background=CARD_BG,
                     foreground=KEEP).pack(anchor="w")
        self.cards.append((card, status))
        return card

    def _refresh_cards(self):
        sel = self.keep_var.get()
        verb = "quarantena" if self.mode_var.get() == "quarantine" else "ELIMINA"
        for j, (card, status) in enumerate(self.cards):
            if sel == SKIP:
                color, text = NEUTRAL, "resta (gruppo non toccato)"
            elif j == sel:
                color, text = KEEP, "✔ TIENI"
            else:
                color, text = REMOVE, f"✖ {verb}"
            card.configure(highlightbackground=color, highlightcolor=color)
            status.configure(text=text, foreground=color)

    def _select(self, j: int):
        paths = self.groups[self.idx][1]
        if j == SKIP or 0 <= j < len(paths):
            self.keep_var.set(j)
            self._on_choice()

    def _on_choice(self):
        h, paths = self.groups[self.idx]
        v = self.keep_var.get()
        self.choices[h] = None if v == SKIP else paths[v]
        self._refresh_cards()
        self._refresh_list_row(self.idx)
        self._update_summary()

    def _on_mode(self):
        self._refresh_cards()
        self._update_summary()

    def prev(self):
        if self.idx > 0:
            self._show_group(self.idx - 1)

    def next(self):
        if self.idx + 1 < len(self.groups):
            self._show_group(self.idx + 1)

    # ---------- riepilogo e azioni ----------

    def _totals(self) -> Tuple[int, int, int]:
        files = size = skipped = 0
        for h, paths in self.groups:
            if self.choices[h] is None:
                skipped += 1
            else:
                files += len(paths) - 1
                size += self.sizes[h] * (len(paths) - 1)
        return files, size, skipped

    def _update_summary(self):
        files, size, skipped = self._totals()
        verb = "in quarantena" if self.mode_var.get() == "quarantine" else "da eliminare"
        self.summary.configure(text=f"{files} copie {verb} · {human_size(size)} · {skipped} gruppi non toccati")

    def apply(self):
        files, size, skipped = self._totals()
        if files == 0:
            messagebox.showinfo("Nessuna azione", "Non c'è nessuna copia da rimuovere.", parent=self.root)
            return
        delete = self.mode_var.get() == "delete"
        action = "ELIMINATE DEFINITIVAMENTE" if delete else "spostate in quarantena"
        msg = (f"{files} copie ({human_size(size)}) verranno {action}.\n"
               f"{skipped} gruppi verranno lasciati com'erano.\n\n"
               "I sidecar (.xmp, .aae, …) seguono le rispettive foto, tranne quelli\n"
               "condivisi con un altro file che resta.\n\nProcedo?")
        if messagebox.askyesno("Conferma", msg, icon="warning" if delete else "question", parent=self.root):
            self.result = (dict(self.choices), self.mode_var.get())
            self._close()

    def cancel(self):
        if messagebox.askyesno("Chiudi", "Chiudere senza modificare nessun file?", parent=self.root):
            self._close()

    def _close(self):
        self._closed = True
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.root.destroy()

    # ---------- miniature ----------

    def _request_thumbs(self, paths: List[Path]):
        for p in paths:
            if p in self.thumb_cache or p in self.pending:
                continue
            self.pending.add(p)
            self.pool.submit(self._load_thumb, p)

    def _load_thumb(self, p: Path):
        try:
            img = thumbnails.make_thumbnail(p, THUMB_SIZE, self.exiftool)
        except Exception:
            img = None
        self.queue.put((p, img))

    def _poll_thumbs(self):
        if self._closed:
            return
        try:
            while True:
                p, img = self.queue.get_nowait()
                self.pending.discard(p)
                self.thumb_cache[p] = ImageTk.PhotoImage(img) if img is not None else None
                lbl = self.thumb_labels.get(p)
                if lbl is not None and lbl.winfo_exists():
                    self._apply_thumb(lbl, p)
        except queue.Empty:
            pass
        self.root.after(50, self._poll_thumbs)

    def _apply_thumb(self, lbl: tk.Label, p: Path):
        photo = self.thumb_cache.get(p)
        if photo is None:
            lbl.configure(image="", text=thumbnails.placeholder_text(p))
        else:
            lbl.configure(image=photo, text="")


def review_duplicates(base: Path, dup_groups: DupGroups, exiftool: Optional[str]) -> Optional[Result]:
    """Apre la finestra e blocca finché l'utente non applica o annulla.
    Ritorna (scelte, modo) oppure None se annullato."""
    if not dup_groups:
        return None
    enable_dpi_awareness()
    root = tk.Tk()
    app = DuplicateReviewer(root, base, dup_groups, exiftool)
    root.mainloop()
    return app.result
