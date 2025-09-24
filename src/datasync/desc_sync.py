#course_introduce数据提取
import csv
import html
import re

import pymysql
import os
from dotenv import load_dotenv
load_dotenv()
from datasync.utils import MysqlReader, Neo4jWriter
from configuration import config

class DESCSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
    def process_data(self):
        #course_info的数据
        sql = """
            select 
                id course_id,
                course_introduce text
            from course_info
        """
        course_data = self.mysql_reader.read(sql)
        with open(config.SYNC_DATA_DIR / "course_data.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=course_data[0].keys())
            writer.writeheader()  # 写表头
            writer.writerows(course_data)  # 写多行

        #chapter_info的数据
        sql = """
            select 
                id chapter_id,
                chapter_name text
            from chapter_info
        """
        chapter_data = self.mysql_reader.read(sql)
        with open(config.SYNC_DATA_DIR /"chapter_data.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=chapter_data[0].keys())
            writer.writeheader()  # 写表头
            writer.writerows(chapter_data)  # 写多行

        #test_question_info的数据
        sql = """
            select 
                id question_id,
                question_txt text
            from test_question_info
        """
        question_data = self.mysql_reader.read(sql)
        for item in question_data:
            item['text'] = self.strip_tags(item['text'])
        with open(config.SYNC_DATA_DIR /"question_data.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=question_data[0].keys())
            writer.writeheader()  # 写表头
            writer.writerows(question_data)  # 写多行

    def strip_tags(self,text: str) -> str:
        """
        删除 HTML 标签并解码实体
        """
        decoded = html.unescape(text)
        # 第二步：删除标签
        no_tags = re.sub(r'<.*?>', '', decoded)
        no_tags = no_tags.replace(' ', ' ')
        no_tags = no_tags.replace('...', '{...}')
        return no_tags.replace(' ','')

if __name__ == '__main__':
    # DESCSynchronizer().process_data()
    text = "<p>关于下列代码说法正确的是：（）。</p><p>public static void main(String[] args) {</p><p>int words = 40;</p><p>System.out.println(words);</p><p>System.out.println(computers);</p><p>words = 67.9;</p><p>}</p>"
    decoded = html.unescape(text)
    no_tags = re.sub(r'<.*?>', '', decoded)
    print(no_tags)
















