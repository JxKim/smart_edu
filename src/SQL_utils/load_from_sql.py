# 本py文件用于加载初始的 在sql中读取到的数据
import pymysql
from configurations import config

def sql_loader():
    connection=pymysql.connect(**config.SQL_CONFIG)
    with connection.cursor() as cursor:
        # language=sql
        sql="""
            select 
                course_introduce as text 
            from 
            course_info
        """
        cursor.execute(sql)
        results=cursor.fetchall()
        return results

if __name__=="__main__":
    results=sql_loader()
    results=[res[0] for res in results]
    course_introduce_results="\n".join(results)

    with open(file=config.SQL_LOADER_PATH/"course_introduce.txt",mode="w",encoding="utf-8") as f:
        f.write(course_introduce_results)
