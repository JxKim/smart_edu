from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent.parent
RAW_DATA_DIR = ROOT_PATH / 'data'
CHECKPOIT_DIR = ROOT_PATH / 'checkpoints'
LOGS_DIR = ROOT_PATH / 'logs'
WEB_STATIC_DIR = ROOT_PATH / 'src' / 'web' / 'static'

MODEL_NAME = 'google-bert/bert-base-chinese'
LABELS = ['B','I','O']

MYSQL_CONFIGS={
    'host': 'localhost',
    'port': 3306,
    'user' : 'root',
    'password' : '123456',
    'database': 'smart_edu'
}

NEO4J_CONFIG = {
    'uri':'neo4j://localhost:7687',
    'auth':('neo4j','1111111q')
}