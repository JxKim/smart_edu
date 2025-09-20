import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor
from configuration.config import *

class MysqlReader:
    def __init__(self):
        self.con = pymysql.connect(**MYSQL_CONFIGS)
        self.cur = self.con.cursor(DictCursor)

    def read(self, sql: object) -> object:
        self.cur.execute(sql)
        return self.cur.fetchall()

class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**NEO4J_CONFIG)

    def build_nodes(self, label, properties):
        cypher = f'''
            UNWIND $batch AS item
            MERGE (:{label} {{id:item.id,name:item.name}})
        '''
        self.driver.execute_query(cypher, batch=properties)

    def build_relations(self, start_label, type, end_label, relations, other=''):
        if other == '' :
            cypher = f'''
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.s_id}}),(end:{end_label} {{id:item.e_id}})
                MERGE (start)-[:{type}]->(end)
            '''
        else:
            cypher = f'''
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.s_id}}),(end:{end_label} {{id:item.e_id}})
                MERGE (start)-[:{type} {{{other}:item.other}}]->(end)
            '''
        self.driver.execute_query(cypher, batch=relations)

if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    sql = """
          select id,
                 subject_name name
          from base_subject_info
          """
    c1 = mysql_reader.read(sql)
    print(c1)
    neo4j_writer.build_nodes('SubjectInfo', c1)

    sql = """
          select id,
                 course_name name
          from course_info
          """
    c1 = mysql_reader.read(sql)
    print(c1)
    neo4j_writer.build_nodes('CourseInfo', c1)

    sql = '''
        select id,
               category_name name
        from base_category_info
    '''
    c2 = mysql_reader.read(sql)
    print(c2)
    neo4j_writer.build_nodes('CategoryInfo', c2)

    sql = '''
            select id,
                   category_name name
            from base_category_info
        '''
    c2 = mysql_reader.read(sql)
    print(c2)
    neo4j_writer.build_nodes('CategoryInfo', c2)

    sql = '''
        select id s_id,
               category_id e_id
        from base_subject_info
    '''
    c2_to_c1 = mysql_reader.read(sql)
    neo4j_writer.build_relations('SubjectInfo', 'Belong', 'CategoryInfo', c2_to_c1)
