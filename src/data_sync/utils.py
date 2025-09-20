import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from src.config import config


class MySqlReader:
    def __init__(self):
        self.conn=pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor=self.conn.cursor(DictCursor)

    def read(self,sql):
        self.cursor.execute(sql)
        res=self.cursor.fetchall()
        return res

    def get_all_create_table_info(self):
        sql="""
        select t1.TABLE_NAME
        from information_schema.TABLES as t1
        where t1.TABLE_SCHEMA='smart_edu'
        """
        self.cursor.execute(sql)
        res=self.cursor.fetchall()
        for table in res:
            table_name=table['TABLE_NAME']
            self.cursor.execute(f"show create table smart_edu.{table_name}")
            ans=self.cursor.fetchall()
            print(ans[0]["Create Table"])

    def getAllCourseIntroduction(self):
        sql="""
        select course_introduce
        from course_info
        """
        res = self.read(sql)
        introduces=[]
        for res_dict in res:
            print(res_dict['course_introduce'])
            # introduces.append(res_dict['course_introduce'])
        # print(introduces)

    def getAllChapterName(self):
        sql="""
        select chapter_name as name
        from chapter_info
        """
        res=self.read(sql)
        for res_dict in res:
            print(res_dict['name'])

    def getAllQuestionName(self):
        sql="""
        select question_txt as name
        from test_question_info
        """
        res=self.read(sql)
        for res_dict in res:
            print(res_dict['name'])

class Neo4jWriter:
    def __init__(self):
        self.driver=GraphDatabase.driver(**config.NEO4J_CONFIG)

    def read(self, cypher,batch):
        res = self.driver.execute_query(cypher,batch=batch)
        return res

    def writeCustomNodeOrRelation(self,cypher,properties):
        self.driver.execute_query(cypher, batch=properties)

    def writeNode(self,label,properties):
        cypher = f"""
                UNWIND $batch AS item
                MERGE (:{label}{{id:item.id,name:item.name}})
                """
        self.driver.execute_query(cypher,batch=properties)

    def writeRelation(self,start_label,end_label,type,relations):
        cypher=f"""
            UNWIND $batch AS item
            MATCH (start:{start_label}{{id:item.start_id}}), (end:{end_label}{{id:item.end_id}})
            MERGE (start)-[:{type}]->(end)
        """

        self.driver.execute_query(cypher,batch=relations)



if __name__=="__main__":
    sql_reader=MySqlReader()
    neo4j_writer=Neo4jWriter()

    # sql_reader.read()
    # sql_reader.get_all_create_table_info()
    # sql_reader.getAllCourseIntroduction()
    # sql_reader.getAllChapterName()
    sql_reader.getAllQuestionName()