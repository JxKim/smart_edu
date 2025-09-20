import pymysql
from pymysql.cursors import DictCursor
from configuration import config

connection = pymysql.connect(**config.MYSQL_CONFIG)
try:
    cursor = connection.cursor(DictCursor)
    sql = """
        select course_introduce
        from course_info 
        where deleted = '0'  
    """
    cursor.execute(sql)
    results = cursor.fetchall()

    course_texts = []
    for row in results:
        text = row.get('course_introduce', '')
        if text:
            course_texts.append(text)
            print(f"{text}")

    print(f"\n共提取到 {len(course_texts)} 条课程介绍文本")
finally:
    cursor.close()
    connection.close()
