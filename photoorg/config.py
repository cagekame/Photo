"""Costanti e impostazioni condivise."""
from dataclasses import dataclass

PHOTO_EXT = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".dng", ".nef", ".cr2", ".cr3", ".arw", ".rw2", ".orf"}
RAW_EXT = {".dng", ".nef", ".cr2", ".cr3", ".arw", ".rw2", ".orf"}
VIDEO_EXT = {".mp4", ".mov", ".m4v", ".avi", ".mts", ".m2ts", ".3gp", ".mkv"}
ALL_EXT = PHOTO_EXT | VIDEO_EXT
SIDECAR_EXT = {".aae", ".xmp", ".thm", ".lrv"}

DATE_KEYS = [
    "SubSecDateTimeOriginal", "DateTimeOriginal",
    "SubSecCreateDate", "CreateDate",
    "XMP:DateCreated", "Photoshop:DateCreated", "IPTC:DateCreated",
    "MediaCreateDate", "TrackCreateDate", "CreationDate",
    "ModifyDate",
    "FileCreateDate", "FileModifyDate",
]

REPORT_FILE = "duplicati_report.txt"
DUP_SCAN_REPORT = "duplicati_scan.txt"
DUP_SCAN_NEAR_REPORT = "duplicati_scan_near.txt"
DUP_SCAN_JSONL = "dup_scan.jsonl"
DUP_ACTION_LOG = "duplicati_action.txt"
DUP_ACTIONS_JSONL = "dup_actions.jsonl"
CONFIG_FILE = "exiftool_path.txt"
CHECKPOINT_FILE = "organize_checkpoint.json"
QUARANTINE_PREFIX = "_QuarantenaDuplicati_"


@dataclass
class Settings:
    batch_exif_size: int = 25
    prehash_bytes: int = 4 * 1024 * 1024
    warn_on_mtime: bool = True
    log_picked_date: bool = True
    normalize_to_local: bool = False
    normalize_to_utc: bool = False
    quicktime_utc: bool = False
    use_shell_fallback: bool = True


# Istanza unica, modificata da cli.py in base ai flag
settings = Settings()
