from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"
VOCAB_FILE=DATA_DIR / "vocab.txt"


MODEL_NAME = "google-bert/bert-base-chinese"
LABELS = ['B', 'I', 'O']

MYSQL_CONFIG={
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'db': 'ai_edu',
    'charset': 'utf8mb4',
}

NEO4J_CONFIG = {
    "uri": "neo4j://host.docker.internal:7687",
    # "uri": "neo4j://127.0.0.1:7687",
    'auth':("neo4j","neo4jneo4j")
}
