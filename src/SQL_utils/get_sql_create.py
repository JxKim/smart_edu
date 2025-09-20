import pymysql
from pymysql.cursors import DictCursor

from configurations import config


# 建立和sql的连接
connection = pymysql.connect(**config.SQL_CONFIG)
# 建立游标对象
cursor=connection.cursor(DictCursor)
# 我要在原信息的表，查询所有的表名，但是必须是ai_edu这个数据库的
# language=SQL
sql="""
    SELECT 
    ORIGIN.TABLE_NAME
    FROM
    information_schema.TABLES AS ORIGIN
    WHERE 
    ORIGIN.TABLE_SCHEMA = 'ai_edu'
"""

cursor.execute(sql)
results=cursor.fetchall()

final_res=[]
# 拿到所有的结果
for table in results:
    table_name=table["TABLE_NAME"]
    sql=f"""
        show create table ai_edu.{table_name}
    """
    cursor.execute(sql)
    create_table_result = cursor.fetchone()
    final_res.append(create_table_result["Create Table"])

print(final_res)