# Guida Rapida — `organize_photos_v5.py`

Questa guida spiega **come usare lo script**, **cosa fanno i flag CLI** e fornisce esempi pratici.
Versione script: **v5** (con gestione sidecar, near-duplicates video, checkpoint, log JSONL).

---

## 1) Cosa fa lo script

* **Organizza foto e video** in cartelle `YYYY/MM` usando **la data originale** dai metadati (senza conversioni di fuso orario, impostazione di default).
* **Rileva duplicati identici** (dimensione → pre-hash → SHA1).
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

* **Python 3**.
* **exiftool** (per leggere i metadati). Lo script lo cerca automaticamente o chiede il percorso.
* *(Opzionale)* **ffprobe** per la funzione di **near-duplicates video**.

  * Verifica: esegui `ffprobe -version` in shell. Se risponde con una versione, è disponibile.

> Se `ffprobe` non è installato, lo script **salta** automaticamente il controllo dei near-duplicates e continua normalmente.

---

## 3) Come avviare lo script

Apri un terminale **nella cartella** che vuoi organizzare ed esegui:

* **Windows (PowerShell/CMD)**

  ```powershell
  python organize_photos_v5.py
  ```

* **macOS / Linux (Terminale)**

  ```bash
  python3 organize_photos_v5.py
  ```

### Modalità interattiva

All’avvio chiede:

1. Se vuoi fare una **scansione duplicati** (senza spostare nulla).
2. In caso affermativo, se includere le **sottocartelle**.
3. Dopo la scansione, se **risolvere i duplicati** (quarantena o eliminazione).
4. Se **non** fai la scansione duplicati, parte l’**organizzazione**: puoi scegliere **dry-run** (simula) e/o un **batch** limitato di file.

---

## 4) Flag CLI (cosa sono e come usarli)

Puoi passare opzioni alla riga di comando per cambiare alcuni comportamenti.
Esempio (macOS/Linux):

```bash
python3 organize_photos_v5.py --utc --prehash-bytes 16777216
```

Esempio (Windows):

```powershell
python organize_photos_v5.py --utc --prehash-bytes 16777216
```

### Tabella dei flag

| Flag                    | A cosa serve                                          | Quando usarlo                                                                                            | Note                                                      |
| ----------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `--no-local-tz`         | **Non** convertire al fuso locale                     | **Di default** lo script già non converte; questo flag è utile solo se in futuro riattivi la conversione | Ridondante con la configurazione attuale (no conversione) |
| `--utc`                 | Converte tutte le date in **UTC**                     | Se vuoi una regola unica e coerente tra più PC/paesi                                                     | Sovrascrive `--no-local-tz`                               |
| `--batch-exif N`        | Dimensione batch per lettura metadati                 | Per ottimizzare le performance                                                                           | Default: `25`; su PC/SSD veloci prova `50` o `100`        |
| `--prehash-bytes BYTES` | Byte letti per **pre-hash** nella scansione duplicati | Tanti video grossi → alza a 8–16MB                                                                       | 8MB = `8388608`, 16MB = `16777216`                        |
| `--disable-near-dup`    | **Disattiva** i near-duplicates video                 | Se non vuoi la funzione o non hai `ffprobe`                                                              | Di default la funzione è **attiva**                       |
| `--no-shell-fallback`   | Disabilita il fallback `shell=True` su Windows        | Massima rigidità/sicurezza                                                                               | Utile se exiftool è affidabile nel PATH                   |
| `--qt-utc`              | Interpreta tempi QuickTime come **UTC**               | Solo se sai che ti serve questa semantica                                                                | Altrimenti lascia disattivo                               |

> **Fuso orario (impostazione corrente):** lo script usa **sempre la data originale** dei metadati. Niente conversione al locale o a UTC, a meno che non passi `--utc`.

---

## 5) Esempi pratici

* **Scansione duplicati** (con near-duplicates video attivo se c’è `ffprobe`), includendo sottocartelle:

  ```bash
  python3 organize_photos_v5.py
  # dopo la scansione puoi scegliere 1) Quarantena o 2) Eliminazione
  ```

* **Organizzazione con dry-run** e **batch** di 200 file (simula senza spostare):

  ```bash
  python3 organize_photos_v5.py
  # scegli "N" quando chiede la scansione duplicati
  # poi "s" al dry-run, e inserisci 200 al batch
  ```

* **Organizzazione reale**, date in **UTC** e pre-hash a **16MB**:

  ```bash
  python3 organize_photos_v5.py --utc --prehash-bytes 16777216
  # scegli "N" alla scansione duplicati, e "n" al dry-run
  ```

* **Disattivare** i near-duplicates video (in un run specifico):

  ```bash
  python3 organize_photos_v5.py --disable-near-dup
  ```

---

## 6) Output e file generati

* `duplicati_scan.txt` — report duplicati (umano)
* `dup_scan.jsonl` — report duplicati (machine-readable)
* `duplicati_scan_near.txt` — near-duplicates video (se attivo e disponibile `ffprobe`)
* `duplicati_action.txt` — log azioni su duplicati (quarantena/elimina)
* `dup_actions.jsonl` — stesso in JSONL
* `duplicati_report.txt` — duplicati identici trovati in fase organizzazione
* `organize_checkpoint.json` — checkpoint per ripartenza
* **Cartelle finali**: `YYYY/MM/`
* **Quarantena**: `_QuarantenaDuplicati_YYYYMMDD_HHMMSS$/<SHA1>/`

---

## 7) Note importanti & suggerimenti

* **Sidecar**: vengono spostati assieme al file principale (stesso basename).
* **Keeper** (duplicati): tiene la copia con **data metadati più vecchia**; in mancanza usa **mtime**.
* **Near-duplicates** video: sono **indicazioni da verificare** (stesso contenuto ma hash diversi). Lo script non li elimina automaticamente: controlla il file `*_near.txt`.
* **Sicurezza**: preferisci la **Quarantena** prima di eliminare definitivamente.
* **Performance**: su SSD e PC moderni, `--batch-exif 50` e `--prehash-bytes 16777216` funzionano bene.
* **Backup**: prima di azioni massive, valuta un backup o fai un run con **dry-run**.

---

## 8) Risoluzione problemi (FAQ)

* **Errore `return outside function`** → qualche riga fuori indentazione dentro `parse_args()`. Ricontrolla che tutte le `ap.add_argument(...)` e il `return` siano rientrati **dentro** la funzione.
* **`exiftool non trovato`** → metti `exiftool` nel PATH o indica il percorso quando lo script te lo chiede; verrà salvato in `exiftool_path.txt`.
* **`ffprobe` non trovato** → viene mostrato un messaggio e i near-duplicates vengono **saltati** (lo script continua).
* **Sidecar non spostati** → assicurati che i sidecar stiano **accanto** al file sorgente (stesso basename).

---

## 9) Scelte predefinite (coerenti con le tue preferenze)

* **Data originale sempre** (nessuna conversione di fuso) sia per **foto** che per **video**.
* **Near-duplicates video**: **attivi di default** (se c’è `ffprobe`).
* `-api QuickTimeUTC` **disattivo di default** (usalo solo se ti serve esplicitamente con `--qt-utc`).

---

**Buon lavoro!** Se vuoi, posso personalizzare questa guida per il tuo ambiente (Windows/NAS, SSD/HDD, dimensione media dei file) con esempi *taylor-made*.
