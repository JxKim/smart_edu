import pymysql
from pymysql.cursors import DictCursor
from configuration import config
import pandas as pd

class MysqlReader:
    def __init__(self):
        self.connection=pymysql.connect(**config.MYSQL_CONFIG)
        #用pymysql连接数据库，初始化时就连接
        self.cursor=self.connection.cursor(DictCursor)
        #这里用cursor处理查询语句
    def read(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


if __name__=='__main__':
    reader = MysqlReader()
    def export_to_csv(path,sql):
        rows = reader.read(sql)
        df = pd.DataFrame(rows)
        df.to_csv(path, index=False)


    sql_course_introduce = "SELECT course_introduce as text FROM course_info"
    sql_chapter_name = "SELECT text FROM chapter_info"
    sql_video_name = "SELECT video_name as text FROM video_info"
    export_to_csv(config.COURSE_INTRODUCE_FROM_SQL / 'course_introduce.csv',sql_course_introduce)
    export_to_csv(config.COURSE_INTRODUCE_FROM_SQL / 'chapter_name.csv',sql_chapter_name)
    export_to_csv(config.COURSE_INTRODUCE_FROM_SQL / 'video_name.csv',sql_video_name)


