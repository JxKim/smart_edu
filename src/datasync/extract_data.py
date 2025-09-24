#❌这个不行

import pymysql

from configuration import config

conn = pymysql.connect(**config.SQL_CONFIG)
cursor = conn.cursor() #输出格式为元组

#从chapter_info抽取chapter_name
sql_table1 = '''select chapter_name from chapter_info'''
cursor.execute(sql_table1)
results1 = cursor.fetchall()
#(('01-尚硅谷-Flume-课程介绍',), ('02-尚硅谷-Flume-学习任务',), ('03-尚硅谷-Flume-概念',))


#从course_info抽取course_introduce
sql_table2 = '''select course_introduce from course_info'''
cursor.execute(sql_table2)
results2 = cursor.fetchall()

cursor.close()
conn.close()

with open(str(config.DATA_DIR/'desc.txt'),'w',encoding='utf-8') as f:
    for row in results1:
        f.write(row[0]+'\n')

    for row in results2:
        f.write(row[0]+'\n')

print('sql数据抽取成功~')



