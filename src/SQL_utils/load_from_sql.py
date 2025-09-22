# 本py文件用于加载初始的 在sql中读取到的数据
import pymysql
from configurations import config
import random

# 这个读取的是少样本的数据
def sql_loader_less():
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

# 这个读取的是大量样本的数据
def sql_loader_large():
    connection=pymysql.connect(**config.SQL_CONFIG)
    with connection.cursor() as cursor:
        # language=sql
        sql="""
            select
                chapter_name as text 
            from 
            chapter_info
        """
        cursor.execute(sql)
        results=cursor.fetchall()
        return results

if __name__=="__main__":
    # 将读取到的少量数据写入文件
    # results=sql_loader_less()
    # results=[res[0] for res in results]
    # course_introduce_results="\n".join(results)
    #
    # with open(file=config.SQL_LOADER_PATH/"course_introduce.txt",mode="w",encoding="utf-8") as f:
    #     f.write(course_introduce_results)

    # 将读取到的大量数据写入文件
    results = sql_loader_large()
    results = [res[0] for res in results]
    results=random.sample(results, 500)

    chapter_name_results = "\n".join(results)

    with open(file=config.SQL_LOADER_PATH / "chapter_name.txt", mode="w", encoding="utf-8") as f:
        f.write(chapter_name_results)
