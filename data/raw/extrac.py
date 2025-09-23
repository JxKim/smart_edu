from os import write

import pymysql
from pymysql.cursors import DictCursor

from src.configuration import config


class MysqlReaderWrite:
    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()



class Extractor:
    def __init__(self):
        self.mysql_reader = MysqlReaderWrite()

    def extra_chart(self):
        sql = """
        select
             course_introduce
        from course_info
        """
        result = self.mysql_reader.read(sql)
        return result

    def write(self,result):
        # 只提取课程介绍
        texts = [item['course_introduce'] for item in result]

        # 将数据写入文本文件
        with open('label-studio.txt', 'w', encoding='utf-8') as f:
            for text in texts:
                f.write(text + '\n')  # 使用两个换行符分隔不同文本

        print("课程介绍已保存到 label-studio.txt 文件中。")



if __name__ == '__main__':
    extractor = Extractor()
    result = extractor.extra_chart()
    extractor.write(result)


