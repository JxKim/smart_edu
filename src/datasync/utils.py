import os

import pymysql
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

load_dotenv()
host = os.getenv('MYSQL_HOST')
port = int(os.getenv('MYSQL_PORT'))
user = os.getenv('MYSQL_USER')
password = os.getenv('MYSQL_PASSWORD')
database = os.getenv('MYSQL_DATABASE')
class MysqlUtil:
    def __init__(self):
        self.conn = pymysql.Connection(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.conn.cursor(DictCursor)
    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

class Neo4jUtil:
    def __init__(self):
        self.driver = GraphDatabase.driver(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')))

    def writer_nodes(self, label, properties):
        cypher = f"""
        UNWIND $batch AS item
        MERGE (n:{label}{{id: item.id}})
        SET n += item
        """
        self.driver.execute_query(cypher, {"batch": properties})
    def writer_relations(self, relation_type, start_label, end_label, relations):
        cypher = f"""
        UNWIND $batch AS item
        MATCH (a:{start_label}{{id: item.start_id}}), (b:{end_label}{{id: item.end_id}})
        MERGE (a)-[:{relation_type}]->(b)
        """
        self.driver.execute_query(cypher, {"batch": relations})
    def writer_relations_attr(self, relation_type, start_label, end_label, relations:list[dict]):
        cypher = f"""
        UNWIND $batch AS item
        MATCH (a:{start_label}{{id: item.start_id}}), (b:{end_label}{{id: item.end_id}})
        MERGE (a)-[r:{relation_type}]->(b)
        SET r += item.props
        """
        relations_attr = []
        for relation in relations:
            relation_attr = {
                "start_id": relation.pop('start_id'),
                "end_id": relation.pop('end_id'),
                "props": relation
            }
            relations_attr.append(relation_attr)
        self.driver.execute_query(cypher, {"batch": relations_attr})

if __name__ == "__main__":
    neo4j_writer = Neo4jUtil()