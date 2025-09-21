from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"

CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"

MODEL_NAME = "google-bert/bert-base-chinese"

LABELS = ['B', 'I', 'O']

from dotenv import load_dotenv
import os

load_dotenv()  # 加载 .env 文件

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

NEO4J_CONFIG = {
    'uri': os.getenv('NEO4J_URI'),
    'auth': (os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
}

API_KEY = os.getenv('API_KEY')
print(API_KEY)
