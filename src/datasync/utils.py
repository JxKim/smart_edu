from dotenv import load_dotenv
load_dotenv()
import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config


class MysqlReader:
    def __init__(self):
        self.conn = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.conn.cursor(DictCursor)

    def read(self,sql,arg=None):
        self.cursor.execute(sql,arg)
        return self.cursor.fetchall()

class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def writer_nodes(self,label,properties):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id,name:item.name}})
        """
        self.driver.execute_query(cypher, {"batch": properties})

    def writer_nodes_with_three_properties(self,label,properties):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.uid,birthday:item.birthday,gender:item.gender}})
        """
        self.driver.execute_query(cypher, {"batch": properties})

    def writer_relations(self,type,start_node,end_node,relations):
        cypher = f"""
            UNWIND $batch AS item
            MATCH (a:{start_node} {{id:item.start_id}}),(b:{end_node} {{id:item.end_id}})
            MERGE (a)-[:{type}]->(b)
             """
        self.driver.execute_query(cypher, {"batch": relations})

    def writer_relations_with_property(self,type,start_node,end_node,property,relations):
        cypher = f"""
            UNWIND $batch AS item
            MATCH (a:{start_node} {{id:item.start_id}}),(b:{end_node} {{id:item.end_id}})
            MERGE (a)-[:{type} {{{property}:item.property}}]->(b)
             """
        self.driver.execute_query(cypher, {"batch": relations})

    def writer_relations_two_property(self,type,start_node,end_node,property1,property2,relations):
        cypher = f"""
            UNWIND $batch AS item
            MATCH (a:{start_node} {{id:item.start_id}}),(b:{end_node} {{id:item.end_id}})
            MERGE (a)-[:{type} {{{property1}:item.property1,{property2}: item.property2}}]->(b)
             """
        self.driver.execute_query(cypher, {"batch": relations})

if __name__ == '__main__':
    sql = """
        select 
            *
        from test_question_info
        limit 5
    """

    mysql_reader = MysqlReader()
    data = mysql_reader.read(sql)
    # print(data)
    question_txt = [item['question_txt'] for item in data]
    # print(question_txt)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(question_txt[1], 'html.parser')
    print(soup)
    print(soup.get_text())


