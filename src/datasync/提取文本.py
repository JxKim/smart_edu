from configuration import config

# 从 course_info表中抽取course_introduce
sql = """
    select
        course_introduce
    from
        course_info
"""
from datasync.utils import MysqlReader, Neo4jWriter
mysql_reader = MysqlReader()
result = mysql_reader.read(sql)

# 将查询结果写入txt文件
with open( config.DATA_DIR / 'course_introduce.txt', 'w', encoding='utf-8') as f:
    for row in result:
        # 假设 course_introduce 是每一行的第一个字段
        f.write(row['course_introduce'] + '\n')

sql = """
    select
        question_txt
    from
        test_question_info
"""
with open( config.DATA_DIR / 'question.txt', 'w', encoding='utf-8') as f:
    for row in mysql_reader.read(sql):
        f.write(row['question_txt'] + '\n')

sql = """
    select
        chapter_name
    from
        chapter_info
"""
with open( config.DATA_DIR / 'chapter_name.txt', 'w', encoding='utf-8') as f:
    for row in mysql_reader.read(sql):
        f.write(row['chapter_name'] + '\n')
