import pymysql

from pymysql.cursors import DictCursor

connection = pymysql.connect(host="127.0.0.1",port=3306,user="root",password="123456")
cursor = connection.cursor(DictCursor)
cursor.execute("""
        SELECT course_introduce
        FROM ai_study.course_info
    
""")
results = cursor.fetchall()

print(results)
with open("course_introduce.txt", "w", encoding="utf-8") as f:
    for result in results:
        f.write(result["course_introduce"])
        f.write("\n")

