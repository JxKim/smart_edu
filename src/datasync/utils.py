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

    def write_nodes(self, label: str, ):
