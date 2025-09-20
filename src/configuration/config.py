from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent


DATA_DIR = ROOT_DIR / "data"
LOGS_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / 'src'/'web'/'static'

MODEL_NAME = "google-bert/bert-base-chinese"

LABELS = ['B','I','O']





MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'port': 3306,
    'database':'edu'
}


NEO4J_CONFIG = {
    'uri':"neo4j://localhost:7687",
    'auth':('neo4j','Zh091512.')
}