from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"

# MODEL_NAME = "Babelscape/wikineural-multilingual-ner"
MODEL_NAME = 'google-bert/bert-base-chinese'

LABELS = ['B', 'I', 'O']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'ai_edu'
}

NEO4J_CONFIG = {
    'uri': 'neo4j://localhost:7687',
    'auth': ('neo4j', '12345678')
}

API_KEY = 'sk-58940d04b58348a88bf1737c7de1eec7'
