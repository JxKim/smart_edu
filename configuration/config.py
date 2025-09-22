# config.py

import os
from dotenv import load_dotenv

load_dotenv()

# --- MySQL 配置 ---
# 从环境变量中读取数据库连接信息，并组装成字典
MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST", "127.0.0.1"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

# 检查必要的MySQL配置是否存在
if not all([MYSQL_CONFIG['user'], MYSQL_CONFIG['password'], MYSQL_CONFIG['database']]):
    raise ValueError("错误: 数据库配置不完整。请检查 .env 文件中的 MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE。")

print("配置加载成功。") # 这是一个好的调试实践

# --- NEO4J 配置 ---
# 从环境变量中读取数据库连接信息，并组装成字典
NEO4J_CONFIG = {
    'uri': os.getenv("NEO4J_URI", "127.0.0.1"),
    'user': os.getenv("NEO4J_USER"),
    'password': os.getenv("NEO4J_PASSWORD"),
}

#静态文件地址
WEB_STATIC_DIR= 'E:\Python\smart_edu\web\static'
