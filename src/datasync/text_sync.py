import random

import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from datasync.vocab_cleaner import CharVocabCleaner
from src.conf import config
from src.datasync.utils import MySqlReader, Neo4jWriter

mysql_reader = MySqlReader()
neo4j_writer = Neo4jWriter()


sql="""
    select
             chapter_name
    from chapter_info
"""

#将chapter——name写入文件中，后续做label_studio标注
chapter_name=mysql_reader.read_data(sql)
print(chapter_name)

#创建数据清洗管道
vocab_cleaner=CharVocabCleaner(vocab_file=config.VOCAB_FILE)

with open("chapter_name.txt", "w", encoding="utf-8") as f:
    for item in chapter_name:
        cleaned_name=vocab_cleaner.clean_text(item['chapter_name'])
        f.write(cleaned_name + "\n")

# 读取所有章节名
with open("chapter_name.txt", "r", encoding="utf-8") as f:
    chapter_names = f.readlines()


# 随机抽取1000条数据（如果总数量不足1000，则抽取全部）
sample_size = min(1000, len(chapter_names))
random_sample = random.sample(chapter_names, sample_size)

# 写入新文件
with open("random_chapter_names.txt", "w", encoding="utf-8") as f:
    f.writelines(random_sample)

