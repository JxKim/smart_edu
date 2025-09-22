import pymysql
from pymysql.cursors import DictCursor

connection = pymysql.connect(host="127.0.0.1",port=3306,user="root",password="123456")
cursor = connection.cursor()
cursor.execute("""
    select
        t1.TABLE_NAME
    from
        information_schema.TABLES as t1
    where
        t1.TABLE_SCHEMA='edu'
""")
results = cursor.fetchall()

for table in results:
    table_name = table[0]
    cursor.execute(f"show create table edu.{table_name}")
    print(cursor.fetchall()[0][1])
