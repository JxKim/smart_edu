import pymysql
from pymysql.cursors import DictCursor

connection = pymysql.connect(host="127.0.0.1",port=3306,user="root",password="17751817")
cursor = connection.cursor()
cursor.execute("""
    select
        t1.TABLE_NAME
    from
        information_schema.TABLES as t1
    where
        t1.TABLE_SCHEMA='edu_ai'
""")
results = cursor.fetchall()

for table in results:
    table_name = table[0]
    cursor.execute(f"show create table edu_ai.{table_name}")
    print(cursor.fetchall()[0][1])
