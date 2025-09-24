import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor
from sqlalchemy import label

from src.conf import config


class MySqlReader:
    def __init__(self):
        self.connection=pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor=self.connection.cursor(cursor=DictCursor)

    def read_data(self,sql):
        """
        :param sql: sql语句
        """
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()


class Neo4jWriter:
    def __init__(self):
        self.driver=GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self,label:str,properties:list[dict]):
        """
        :param label: 节点标签
        :param properties: 节点属性列表
        """
        cypher=f'''
            unwind $batch as item
            merge (n:{label} {{id:item.id,name:item.name}})
        '''
        self.driver.execute_query(cypher,batch=properties)

    def write_relations(self,start_label:str,end_label:str,type:str,properties:list[dict]):
        """
        :param start_label: 起始节点标签
        :param end_label: 结束节点标签
        :param type: 关系类型
        :param properties: 关系属性列表
        """
        cypher=f'''
            unwind $batch as item
            match (a:{start_label} {{id:item.start_id}}),(b:{end_label} {{id:item.end_id}})
            merge (a)-[:{type}]->(b)
        '''
        self.driver.execute_query(cypher,batch=properties)

if __name__ == '__main__':
    mysql_reader=MySqlReader()
    sql="""
        select  category_id id,
                subject_name name
        from base_subject_info
    """
    subject=mysql_reader.read_data(sql)
    mysql_reader.close()
    neo4j_writer=Neo4jWriter()
    neo4j_writer.write_nodes("subject", subject)





