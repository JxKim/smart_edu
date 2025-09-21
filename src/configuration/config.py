from pathlib import Path

ROOT_DIR=Path(__file__).parent.parent.parent
CHECKPOINT_DIR=ROOT_DIR/'checkpoint'
DATA_DIR=ROOT_DIR/'data'
LOGS_DIR=ROOT_DIR/'logs'
WEB_STATIC_DIR=ROOT_DIR/'src'/'web'/'static'

#NER
LABELS=["B","I","O"]

#pymysql
MYSQL_CONFIG={
    'host':'localhost',
    'user':'root',
    'password':'1234',
    'db':'ai_edu',
}

NEO4J_CONFIG={
    'uri':'bolt://localhost:7687',
    'auth':('neo4j','18706875572.')
}
