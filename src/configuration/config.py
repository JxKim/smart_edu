from pathlib import Path

from torch.export.pt2_archive.constants import MODELS_DIR

ROOT_DIR = Path(__file__).parent.parent.parent

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"
NER_RAW_DIR = DATA_DIR / "ner" / "raw"
SYNC_DATA_DIR = DATA_DIR / "database_sync" / "raw"

MODELS_DIR = ROOT_DIR / "pretrained"/"bert-base-chinese"

LABELS = ['B', 'I', 'O']
