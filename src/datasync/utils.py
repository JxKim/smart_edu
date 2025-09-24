import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config


class MysqlReader:
    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)  # 使用DictCursor返回字典

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def read(self, cypher,batch):
        res = self.driver.execute_query(cypher,batch=batch)
        return res

    def writeCustomNodeOrRelation(self,cypher,properties):
        self.driver.execute_query(cypher, batch=properties)

    def write_node(self, label, properties):
        cypher = f"""
                UNWIND $batch AS item
                MERGE (:{label}{{id:item.id,name:item.name}})
                """
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, start_label, end_label, type, relations):
        cypher = f"""
            UNWIND $batch AS item
            MATCH (start:{start_label}{{id:item.start_id}}), (end:{end_label}{{id:item.end_id}})
            MERGE (start)-[:{type}]->(end)
        """

        self.driver.execute_query(cypher, batch=relations)
