from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR_RAW = ROOT_DIR / 'data'/'raw'
DATA_DIR_PROCESSED = ROOT_DIR / 'data'/'processed'
LOGS_DIR = ROOT_DIR / 'logs'
MODEL_DIR = ROOT_DIR / 'model'
CHECKPOINTS_DIR = ROOT_DIR / 'checkpoints'

LABELS =  ['B','I','O']


MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'db': 'ai_edu',
    'charset': 'utf8mb4'
}

NEO4J_CONFIG = {
    "uri": "neo4j://localhost:7687",
    "auth": ("neo4j", "12345678"),
}