from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent


DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"

MODEL_NAME = "google-bert/bert-base-chinese"

import pymysql
from pymysql.converters import conversions

conv = conversions.copy()
conv[246] = str

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'ai_edu',
    'conv':conv
}

NEO4J_CONFIG = {
    'uri': 'bolt://localhost:7687',
    'auth': ('neo4j', 'Xyz161213....')
}

API_KEY='sk-V9oxDA8LttNG7HkHxVpALpi7tqXEnA6SYzYm0qf72yYC6Hu5'

COURSE_INTRODUCE_FROM_SQL=DATA_DIR / 'raw'

WEB_STATIC_DIR=ROOT_DIR / 'src' / 'web' / 'static'