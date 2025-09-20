import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config


class MysqlReader:
    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)
    def read(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()



class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
    def write_nodes(self,label:str,properties:list[dict]):
        cypher = f"""
            UNWIND $batch As item
            MERGE (n:{label} {{id:item.id,name:item.name}})
        """
        self.driver.execute_query(cypher,batch = properties)

    def write_chapter_knowledge(self,label:str,properties:list[dict]):
        cypher = f"""
                    UNWIND $batch As item
                    MERGE (n:{label} {{id:item.id,name:item.name,pre:item.pre}})
                """
        self.driver.execute_query(cypher, batch=properties)
    def write_knowledge_relations(self):
        cypher = f"""
                    MATCH (start:知识点),(end:知识点) 
                    WHERE start.pre = end.pre AND  split(start.id, "-")[0] = split(end.id, "-")[0]
  AND toInteger(split(start.id, "-")[1]) + 1 = toInteger(split(end.id, "-")[1])
                    MERGE (end)-[:Need]->(start)    
                """
        self.driver.execute_query(cypher)
    def write_relations(self,type:str,start_label,end_label,relations:list[dict]):
        cypher = f"""
            UNWIND $batch As item
            MATCH (start:{start_label}{{id:item.start_id}}),(end:{end_label}{{id:item.end_id}})  
            MERGE (start)-[:{type}]->(end)    
        """
        self.driver.execute_query(cypher,batch = relations)

    def write_course_nodes(self,label:str,properties:list[dict]):
        cypher = f"""
                    UNWIND $batch As item
                    MERGE (n:{label} {{id:item.id,name:item.name,teacher_name:item.teacher_name,price:item.price}}) 
                """
        self.driver.execute_query(cypher, batch=properties)

    def write_student_nodes(self,label:str,properties:list[dict]):
        cypher = f"""
                            UNWIND $batch As item
                            MERGE (n:{label} {{uid:item.id,birth:item.birth,gender:item.gender}}) 
                        """
        self.driver.execute_query(cypher, batch=properties)

    def write_student_favor(self,type:str,start_label,end_label,relations:list[dict]):
        cypher = f"""
                    UNWIND $batch As item
                    MATCH (start:{start_label}{{uid:item.start_id}}),(end:{end_label}{{id:item.end_id}})  
                    MERGE (start)-[:{type}{{时间:item.create_time}}]->(end)    
                """
        self.driver.execute_query(cypher, batch=relations)

    def write_student_question(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                    UNWIND $batch As item
                    MATCH (start:{start_label}{{uid:item.start_id}}),(end:{end_label}{{id:item.end_id}})  
                    MERGE (start)-[:{type}{{是否正确:item.is_correct}}]->(end)    
                """
        self.driver.execute_query(cypher, batch=relations)

    def write_student_watch(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                    UNWIND $batch As item
                    MATCH (start:{start_label}{{uid:item.start_id}}),(end:{end_label}{{id:item.end_id}})  
                    MERGE (start)-[:{type}{{进度:item.progress}}]->(end)    
                """
        self.driver.execute_query(cypher, batch=relations)



if  __name__ == '__main__':
    reader = MysqlReader()
    writer = Neo4jWriter()
    sql = """
                     select user_id start_id,
                     course_id end_id,
                     create_time
                     from favor_info
                            """
    relations = reader.read(sql)
    print(relations)