import re

import pymysql
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

from datasync.utils import MysqlReader, Neo4jWriter

load_dotenv()


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.cursor = self.mysql_reader.cursor

    def generate_node(self):
        # 从数据库中获取所表的描述信息
        res_list = self.selectTableDesc()
        for sql in res_list:
            table_name, fields, decimal_fields = self.parse_create_table(sql)
            # 根据表描述信息生成查询sql语句、cypher语句
            print(table_name)
            sql, cypher = self.generate_sql_and_cypher(table_name, fields, decimal_fields)
            properties = self.mysql_reader.read(sql)
            # 删除为value为null的key
            if table_name != 'region':
                properties = [{k: (v if v is not None else "") for k, v in d.items()} for d in properties]
                # 写入节点
                self.neo4j_writer.write_nodes_with_cypher(cypher, properties)

    @staticmethod
    def parse_create_table(sql: str):
        """
        解析 CREATE TABLE 语句，返回表名和字段列表
        """
        # 表名
        table_match = re.search(r'CREATE TABLE\s+`?(\w+)`?', sql, re.IGNORECASE)
        if not table_match:
            return None, []
        table_name = table_match.group(1)

        # 匹配列定义：列名 + decimal(...)
        # 匹配列定义：列名 + decimal(...)
        pattern = re.compile(r'`?(\w+)`?\s+decimal\([^)]*\)', re.IGNORECASE)
        decimal_fields = pattern.findall(sql)

        # 字段
        fields = []
        for line in sql.splitlines():
            field_match = re.match(r'\s*`(\w+)`', line)
            if field_match:
                field = field_match.group(1)
                # 过滤掉不需要的时间字段
                if field not in {"create_time", "update_time", "operate_time"}:
                    fields.append(field)

        return table_name, fields, decimal_fields

    @staticmethod
    def generate_sql_and_cypher(table_name, fields, decimal_fields):
        """
        根据表名和字段生成 SQL 和 Cypher
        """
        # SQL
        decimal_fields = set(decimal_fields or [])
        select_fields = []

        for f in fields:
            if f in decimal_fields:
                select_fields.append(f"CAST({f} AS CHAR) AS {f}")
            else:
                select_fields.append(f)

        field_str = ", ".join(select_fields)
        sql = f"SELECT {field_str} FROM {table_name};"

        # Cypher
        props = ",".join([f"{f}:item.{f}" for f in fields])
        cypher = f"""
        UNWIND $batch AS item
        MERGE (n:{table_name}{{
          {props}
        }})
        """
        return sql, cypher

    def selectTableDesc(self):

        self.cursor.execute("""
            select
                t1.TABLE_NAME
            from
                information_schema.TABLES as t1
            where
                t1.TABLE_SCHEMA='atguigu'
        """)
        results = self.cursor.fetchall()
        res_list = []
        for table in results:
            table_name = table['TABLE_NAME']
            self.cursor.execute(f"show create table atguigu.{table_name}")
            res_list.append(self.cursor.fetchall()[0]['Create Table'])
        return res_list

    def generate_all_relations(self):
        """
        #总生成关系
        :return:
        """
        self.generate_course_data()
        self.generate_student_behavior()

    def generate_course_data(self):
        # 课程到学科
        sql = "select id start_id,subject_id end_id from course_info "
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'course_info', 'base_subject_info', properties)

        # 学科到分类
        sql = "select id start_id,category_id end_id from base_subject_info "
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'base_subject_info', 'base_category_info', properties)

        # 视频到章节
        sql = "select id start_id,chapter_id end_id from video_info "
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'video_info', 'chapter_info', properties)

        # 章节到课程
        sql = "select id start_id,course_id end_id from chapter_info "
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'chapter_info', 'course_info', properties)

        # 试题到试卷
        sql = """
                select tqi.id start_id,tp.id end_id
                    from test_paper_question tpq
                    join test_paper tp on tp.id=tpq.paper_id
                    join test_question_info tqi on tqi.id=tpq.question_id
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'test_question_info', 'test_paper', properties)

        # 试卷到课程
        sql = "select id start_id,course_id end_id from test_paper "
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'test_paper', 'course_info', properties)

        # 课程到教师
        sql = "select id course_id,teacher teacher_name from course_info "
        properties = self.mysql_reader.read(sql)
        # 创建老师节点
        cypher = """
                    UNWIND $batch AS item
                    MERGE (teacher:teacher_info{teacher_name:item.teacher_name})
                """
        self.neo4j_writer.write_nodes_with_cypher(cypher, properties)
        # 创建关系
        cypher = """
                    UNWIND $batch AS item
                    match (course:course_info{course_id:item.course_id})
                    match (teacher:teacher_info{teacher_name:item.teacher_name})
                    merge (course)-[:HAVE]->(teacher)
                """
        self.neo4j_writer.special_write_relations(cypher, properties)

        # 课程到价格
        sql = "select id course_id, CONCAT(actual_price, '') actual_price from course_info "
        properties = self.mysql_reader.read(sql)
        cypher = """
                    UNWIND $batch AS item
                    MERGE (price:price_info{actual_price:item.actual_price})
                """
        self.neo4j_writer.write_nodes_with_cypher(cypher, properties)
        cypher = """
                    UNWIND $batch AS item
                    match (course:course_info{course_id:item.course_id})
                    match (price:price_info{price:item.price})
                    merge (course)-[:HAVE]->(price)
                """
        self.neo4j_writer.special_write_relations(cypher, properties)

    def generate_student_behavior(self):
        # 学生到课程
        sql = """
        select user_id start_id,
              course_id end_id,
              favor_info.create_time favor_time
            from user_info
            join favor_info on favor_info.user_id=user_info.id
            join course_info on course_info.id=favor_info.course_id
        """
        properties = self.mysql_reader.read(sql)
        cypher = """
                    UNWIND $batch AS item
                    MATCH (start:user_id {id:item.start_id}),(end:course_id {id:item.end_id})
                    MERGE (start)-[:FAVOR{favor_time:item.time}]->(end)
                """
        self.neo4j_writer.special_write_relations(cypher, properties)

        # 学生到试题
        sql = """
        select 
            user_id start_id,
            question_id end_id,
            is_correct     
        from test_exam_question
        """
        properties = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch as item
            MATCH (start:user_info {id:item.start_id}),(end:test_question_info {id:item.end_id}) 
            MERGE (start)-[:ANSWER{is_correct:item.is_correct}]->(end)
        """
        self.neo4j_writer.special_write_relations(cypher, properties)

        # 学生到章节视频
        sql = """
        select 
            user_id start_id,
            course_id end_id,
            position_sec
        from user_chapter_progress
        """
        properties = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch as item
            MATCH (start:user_info {id:item.start_id}),(end:course_info {id:item.end_id}) 
            MERGE (start)-[:WATCH{position_sec:item.position_sec}]->(end)
        """
        self.neo4j_writer.special_write_relations(cypher, properties)


if __name__ == '__main__':
    # TableSynchronizer().generate_node()
    TableSynchronizer().generate_all_relations()
