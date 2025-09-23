import pymysql

connection = pymysql.connect(host='127.0.0.1', port=3306,\
                             user='root', password='123456')
cursor = connection.cursor()
cursor.execute("""
    select
        t1.table_name
    from 
        information_schema.tables as t1
    where
        t1.table_schema = 'ai_edu'
""")
results = cursor.fetchall()

for table in results:
    table_name = table[0]
    cursor.execute(f"show create table ai_edu.{table_name}")
    print(cursor.fetchall()[0][1])