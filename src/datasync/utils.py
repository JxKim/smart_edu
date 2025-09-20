import decimal

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
        rows = self.cursor.fetchall()

        # 自动把 Decimal 转 float，避免 Neo4j 驱动报错
        for row in rows:
            for k, v in row.items():
                if isinstance(v, decimal.Decimal):
                    row[k] = float(v)
        return rows



class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self, label: str, properties: list[dict]):


        # 拼接所有属性
        all_props = ", ".join([f"{k}: item.{k}" for k in properties[0].keys()])
        cypher = f"""
            UNWIND $batch AS item
            CREATE (n:{label})
            SET n += {{{all_props}}}
        """
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, type: str, start_label, end_label, relations: list[dict],
                        start_key: str, end_key: str):
        cypher = f"""
        UNWIND $batch AS item
        MATCH (start:{start_label} {{{start_key}: item.start_id}})
        MATCH (end:{end_label} {{{end_key}: item.end_id}})
        MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, batch=relations)
if __name__ == '__main__':
    pass