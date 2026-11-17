# Guida Rapida — `organize_photos.py`

Questa guida spiega **come usare lo script**, **cosa fanno i flag CLI** e fornisce esempi pratici.

---

## 1) Cosa fa lo script

* **Organizza foto e video** in cartelle `YYYY/MM` usando **la data originale** dai metadati (senza conversioni di fuso orario, impostazione di default).
* **Rileva duplicati identici** (dimensione → pre-hash → SHA1).
* **Finestra di revisione dei duplicati**: mostra le anteprime di ogni gruppo e ti fa scegliere quale copia tenere.
* **(Video) Near-duplicates**: trova probabili duplicati con stesso contenuto ma ricodificati (richiede `ffprobe`).
* **Gestisce i sidecar** (`.xmp`, `.aae`, `.thm`, `.lrv`) sia durante l’organizzazione sia quando risolve i duplicati.
* **Quarantena o elimina** i duplicati in modo sicuro, con **log** in TXT e **JSONL**.
* **Riprende dopo interruzione** grazie al file **checkpoint**.

**Formati supportati** (principali):
Foto: `.jpg, .jpeg, .png, .heic, .heif, .dng, .nef, .cr2, .cr3, .arw, .rw2, .orf`
Video: `.mp4, .mov, .m4v, .avi, .mts, .m2ts, .3gp, .mkv`
Sidecar: `.xmp`, `.aae`, `.thm`, `.lrv`

---

## 2) Requisiti

* **Python 3** (con tkinter, incluso nell'installer standard di Windows).
* **exiftool** (per leggere i metadati). Lo script lo cerca accanto a sé, nel PATH, o chiede il percorso.
* **Pillow** per le anteprime nella finestra dei duplicati: `pip install pillow`
* *(Opzionale)* **pillow-heif** per le anteprime delle foto HEIC/iPhone: `pip install pillow-heif`
* *(Opzionale)* **ffmpeg / ffprobe**: `ffmpeg` per le anteprime dei video, `ffprobe` per i near-duplicates video.

> Senza i componenti opzionali lo script funziona comunque: mostra un riquadro al posto dell'anteprima e salta i near-duplicates.
> Le anteprime dei RAW (`.cr2`, `.nef`, `.arw`, …) vengono estratte con exiftool.

---

## 3) Come avviare lo script

Avvia lo script (anche con doppio clic):

```powershell
python D:\Photo\organize_photos.py
```

All'avvio si apre la **finestra iniziale**, dove scegli tutto in una volta (nel terminale non viene chiesto nulla):

* **Cartella da elaborare** — scrivila o usa *Sfoglia…*. Con `--cartella "E:\Foto"` la trovi già compilata.
* **Cerca i DUPLICATI**
  * *Includi le sottocartelle*
  * *Dopo la scansione*: finestra con le anteprime per scegliere tu (consigliato) · automatico in quarantena · automatico con eliminazione · solo report.
* **ORGANIZZA in cartelle anno/mese**
  * *Solo PROVA*: mostra cosa farebbe senza spostare nulla (attivo di default).
  * *Limita a N file*: vuoto = tutti.

Premi **Avvia** (o Invio). **Esci** (o Esc) chiude senza fare nulla.
Prima di spostare o eliminare file viene sempre chiesta conferma. L'avanzamento e il riepilogo si vedono nel terminale.

### Finestra di revisione dei duplicati

* A sinistra l'elenco dei gruppi (stesso ordine e numerazione di `duplicati_scan.txt`).
* Al centro le anteprime delle copie: bordo **verde** = tieni, **rosso** = quarantena/elimina.
  Per ogni copia vedi cartella, data di modifica e se è già in una cartella anno/mese.
* Per ogni gruppo è già proposta la scelta automatica: devi intervenire solo dove vuoi cambiarla.
* "**Non toccare questo gruppo**" lascia tutte le copie al loro posto.
* In basso scegli **Quarantena** o **Elimina** e premi **Applica scelte…** (chiede conferma). **Annulla** chiude senza toccare nulla.

Scorciatoie: `←` / `→` gruppo precedente/successivo · `1`–`9` scegli la copia · `0` non toccare il gruppo · **doppio clic** su un'anteprima apre il file.

---

## 4) Flag CLI (cosa sono e come usarli)

| Flag                    | A cosa serve                                          | Note                                                      |
| ----------------------- | ----------------------------------------------------- | --------------------------------------------------------- |
| `--cartella PERCORSO`   | Cartella già compilata nella finestra iniziale        | Puoi comunque cambiarla prima di premere Avvia            |
| `--utc`                 | Converte tutte le date in **UTC**                     | Utile per una regola unica tra più PC/paesi               |
| `--no-local-tz`         | **Non** convertire al fuso locale                     | È già il comportamento predefinito                        |
| `--batch-exif N`        | Dimensione batch per lettura metadati                 | Default `25`; su PC/SSD veloci prova `50` o `100`         |
| `--prehash-bytes BYTES` | Byte letti per **pre-hash** nella scansione duplicati | Default 4MB; con tanti video grossi prova `16777216` (16MB) |
| `--disable-near-dup`    | **Disattiva** i near-duplicates video                 | Di default la funzione è attiva (se c'è `ffprobe`)        |
| `--no-shell-fallback`   | Disabilita il fallback `shell=True` su Windows        | Utile se exiftool è affidabile nel PATH                   |
| `--qt-utc`              | Interpreta tempi QuickTime come **UTC**               | Solo se sai che ti serve                                  |

---

## 5) Esempi pratici

* **Rivedere i duplicati** con la finestra, includendo sottocartelle:

  ```powershell
  python D:\Photo\organize_photos.py
  # nella finestra: scegli la cartella, "Cerca i DUPLICATI", lascia le opzioni predefinite, Avvia
  ```

* **Organizzazione con dry-run** e **batch** di 200 file (simula senza spostare):

  ```powershell
  python D:\Photo\organize_photos.py
  # nella finestra: "ORGANIZZA", lascia "Solo PROVA" spuntato, scrivi 200 in "Limita a", Avvia
  ```

* **Organizzazione reale**, date in **UTC** e pre-hash a **16MB**:

  ```powershell
  python D:\Photo\organize_photos.py --utc --prehash-bytes 16777216
  ```

---

## 6) Output e file generati

* `duplicati_scan.txt` — report duplicati (umano)
* `dup_scan.jsonl` — report duplicati (machine-readable)
* `duplicati_scan_near.txt` — near-duplicates video (se attivo e disponibile `ffprobe`)
* `duplicati_action.txt` — log azioni su duplicati (quarantena/elimina)
* `dup_actions.jsonl` — stesso in JSONL
* `duplicati_report.txt` — duplicati identici trovati in fase organizzazione
* `organize_checkpoint.json` — esiste solo se un'organizzazione è stata interrotta
* **Cartelle finali**: `YYYY/MM/`
* **Quarantena**: `_QuarantenaDuplicati_YYYYMMDD_HHMMSS/<SHA1>/`

---

## 7) Note importanti & suggerimenti

* **Organizzazione**: elabora i file nella cartella indicata, **non** nelle sottocartelle (così le cartelle `YYYY/MM` già fatte restano intatte).
  La scansione duplicati invece può essere ricorsiva.
* **Sidecar**: vengono spostati assieme al file principale (stesso nome base, stessa cartella).
  Se un sidecar è condiviso (es. `IMG_1.CR2` + `IMG_1.JPG` + `IMG_1.xmp`) parte insieme all'ultimo dei due file.
* **Keeper automatico** (duplicati): tiene la copia **già organizzata in `YYYY/MM`**; a parità, quella con **mtime più vecchio**, poi il percorso più corto.
* **Sidecar condivisi nei duplicati**: se un sidecar appartiene anche a un altro file che resta, non viene spostato né eliminato.
* Le cartelle `_QuarantenaDuplicati_*` vengono **escluse** dalle scansioni successive.
* **Checkpoint**: se interrompi l'organizzazione (Ctrl+C), rilanciando lo script i file già esaminati vengono saltati.
  Il checkpoint non viene scritto in dry-run e si cancella da solo a fine lavoro.
* **Near-duplicates** video: sono **indicazioni da verificare**. Lo script non li elimina: controlla `duplicati_scan_near.txt`.
* **Sicurezza**: preferisci la **Quarantena** prima di eliminare definitivamente. Prima di azioni massive valuta un backup o un dry-run.

---

## 8) Struttura del codice

`organize_photos.py` è solo il punto di avvio; il codice è nel pacchetto `photoorg/`:

| Modulo              | Contenuto                                                        |
| ------------------- | ---------------------------------------------------------------- |
| `config.py`         | estensioni, nomi dei file di report, impostazioni da CLI         |
| `exif.py`           | ricerca ed esecuzione di exiftool, lettura date, anteprime RAW   |
| `fileutils.py`      | hash, scansione cartelle, sidecar                                |
| `duplicates.py`     | ricerca duplicati, near-duplicates, scelta keeper, quarantena    |
| `organize.py`       | spostamento in `YYYY/MM` e checkpoint                            |
| `thumbnails.py`     | miniature (Pillow, HEIC, RAW, video)                             |
| `gui_duplicates.py` | finestra tkinter di revisione duplicati                          |
| `start_window.py`   | finestra iniziale: cartella e operazione da eseguire             |
| `dialogs.py`        | finestre di conferma/avviso e scelta di exiftool                 |
| `cli.py`            | flag da riga di comando e avvio delle operazioni                 |

---

## 9) Risoluzione problemi (FAQ)

* **`exiftool non trovato`** → metti `exiftool` nel PATH o accanto allo script, o indica il percorso quando lo script te lo chiede; verrà salvato in `exiftool_path.txt`.
* **Anteprime mancanti** → installa `pillow` (e `pillow-heif` per gli HEIC); per i video serve `ffmpeg` nel PATH.
* **`ffprobe` non trovato** → i near-duplicates vengono **saltati** (lo script continua).
* **Sidecar non spostati** → devono stare **accanto** al file sorgente con lo stesso nome base.
