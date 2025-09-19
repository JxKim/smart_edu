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

    def write_nodes(self, label: str, propeprties: list[dict]):
        cypher = f"""
                    UNWIND $batch AS item
                    MERGE (n:{label} {{id:item.id, name:item.name}})
                    """
        self.driver.execute_query(cypher, batch = propeprties)

    def write_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)

if __name__ == '__main__':
    reader = MysqlReader()
    writer = Neo4jWriter()
    sql = "SELECT id, category_name as name FROM base_category_info"
    result = reader.read(sql)
    writer.write_nodes("分类" , result)



