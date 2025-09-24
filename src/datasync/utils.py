import os
from dotenv import load_dotenv

load_dotenv()

import pymysql
from pymysql.cursors import DictCursor
from neo4j import GraphDatabase


class MysqlReader:
    def __init__(self):
        self.connection = pymysql.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            port=int(os.getenv("MYSQL_PORT"))
        )
        self.cursor = self.connection.cursor(DictCursor)

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            uri=os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_AUTH_USER'), os.getenv('NEO4J_AUTH_PASSWORD'))
            # auth=('neo4j','12345678')
        )

    def write_nodes(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id,name:item.name}})
            """
        self.driver.execute_query(cypher, batch=properties)

    def write_nodes_with_cypher(self, cypher, properties: list[dict]):
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)

    def special_write_relations(self, cypher, relations: list[dict]):
        self.driver.execute_query(cypher, batch=relations)
