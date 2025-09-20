import os
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"

MODEL_NAME = "google-bert/bert-base-chinese"

LABELS = ['B', 'I', 'O']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'edu'
}

NEO4J_CONFIG = {
    'uri': 'neo4j://localhost:7687',
    'auth': ('neo4j', 'a13805193942')
}

API_KEY = os.getenv("APIKEY")

if __name__ == '__main__':
    print(API_KEY)
