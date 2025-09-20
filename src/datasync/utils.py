import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config

class MysqlReader:
    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)


    def read(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def write(self,sql,column):
        self.cursor.execute(sql,column)
        self.connection.commit()


class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self, label: str, properties: list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MERGE (n:{label} {{id:item.id, name:item.name}})
        """
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, type:str, start_label, end_label, relations :list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
        MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, batch=relations)

    def write_student_info_nodes(self, label: str, properties: list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MERGE (n:{label} {{id:item.id, name:item.real_name, birthday:item.birthday, gender:item.gender}})
        """
        self.driver.execute_query(cypher, batch=properties)

    def write_student_to_course_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MATCH(start: {start_label} {{id: item.start_id}}), (end:{end_label} {{id:item.end_id}})
        MERGE(start) - [: {type} {{favor_time: item.favor_time}}]->(end)
        """
        self.driver.execute_query(cypher, batch=relations)

    def write_student_to_video_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MATCH(start: {start_label} {{id: item.start_id}}), (end:{end_label} {{id:item.end_id}})
        MERGE(start) - [: {type} {{progress: item.progress, latest_time:item.latest_time}}]->(end)
        """
        self.driver.execute_query(cypher, batch=relations)









if __name__ == "__main__":
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    # 读取Category1的数据
    sql = """
          select id,
                 name
          from base_category1
          """
    category1 = mysql_reader.read(sql)
    print(category1)

    # 写入Category1的数据
    neo4j_writer.write_nodes("Category1", category1)

    # 读取Category2的数据
    sql = """
          select id,
                 name
          from base_category2
          """
    category2 = mysql_reader.read(sql)
    # 写入Category2的数据
    neo4j_writer.write_nodes("Category2", category2)

    # 读取category1和category2的关系
    sql = """
          select id           start_id,
                 category1_id end_id
          from base_category2
          """
    category1_to_category2 = mysql_reader.read(sql)
    print(category1_to_category2)

    # 写入category1和category2的关系
    neo4j_writer.write_relations("Belong", "Category2", "Category1", category1_to_category2)
