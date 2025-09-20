import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config

connection = pymysql.connect(**config.MYSQL_CONFIG)
cursor = connection.cursor(DictCursor)


course_sql = """
        select id, course_introduce
        from course_info;"""
cursor.execute(course_sql)
results = cursor.fetchall()

with open(config.DATA_DIR/'raw'/'course_info.txt', 'w', encoding='utf-8') as f:
    for res in results:
        f.write(str(res['id']) + ',' + res['course_introduce'])
        f.write('\n')

chapter_sql = """
        select id, chapter_name
        from chapter_info;"""
cursor.execute(chapter_sql)
results = cursor.fetchall()

with open(config.DATA_DIR/'raw'/'chapter_info.txt', 'w', encoding='utf-8') as f:
    for res in results:
        f.write(str(res['id']) + ',' + res['chapter_name'])
        f.write('\n')


question_sql = """
        select id, question_txt
        from test_question_info;"""
cursor.execute(question_sql)
results = cursor.fetchall()

with open(config.DATA_DIR/'raw'/'question_info.txt', 'w', encoding='utf-8') as f:
    for res in results:
        f.write(str(res['id']) + ',' + res['question_txt'])
        f.write('\n')

