from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

CHECKPOINTS_DIR = ROOT_DIR / 'checkpoint'
LOGS_DIR = ROOT_DIR / 'logs'
RAW_DIR = ROOT_DIR / 'data' / 'raw'
PROCESSED_DIR = ROOT_DIR / 'data' / 'processed'
BEST_MODEL_DIR = ROOT_DIR / 'best_model'
WEB_STATIC_DIR = ROOT_DIR / 'src' / 'web' / 'static'