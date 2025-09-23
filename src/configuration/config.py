from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
DATA_DIR = ROOT_DIR / 'data'

MODEL_NAME = 'google-bert/bert-base-chinese'
CHECKPOINT_DIR = ROOT_DIR / 'checkpoints'
LOG_DIR = ROOT_DIR / 'logs'

LABELS = ['B', 'I', 'O']

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'gmall'
}

NEO4J_CONFIG = {
    'uri': 'neo4j://localhost:7687',
    'auth': ('neo4j', 'zct123456')
}

API_KEY = 'sk-3dac1d9773aa4add8af5902b701416fc'
BASE_URL="https://dashscope.aliyuncs.com/compatible-mode/v1"