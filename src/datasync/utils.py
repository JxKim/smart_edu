import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config




class MysqlReader:
    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id,name:item.name}})
            """
        self.driver.execute_query(cypher, batch=properties)

    def write_name(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{name:item.name}})
            """
        self.driver.execute_query(cypher, batch=properties)

    def write_price(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id,name:item.name}})
            """
        self.driver.execute_query(cypher, batch=properties)

    def write_user(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id,birthday:item.birthday,gender:item.gender}})
            """
        self.driver.execute_query(cypher, batch=properties)



    def write_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)


    def write_stu_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type} {{favor_time:item.favor_time}}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)

    def write_course_to_teacher_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{name:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)

    def write_course_to_price_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{name:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)




if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()