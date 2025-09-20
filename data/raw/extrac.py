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

    def write(self):
        pass

class Extractor:
    def __init__(self):
        self.mysql_reader = MysqlReaderWrite()

    def extra_chart(self):
        sql = """
        select
            chapter_name name
        from chapter_info
        """
        result = self.mysql_reader.read(sql)


if __name__ == '__main__':
    extractor = Extractor()
    extractor.extra_chart()


