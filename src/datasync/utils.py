from decimal import Decimal
import html2text
import pymysql
from pandas._libs import properties
from pymysql.cursors import DictCursor
from neo4j import GraphDatabase


from configuration import config



class MySqlReader:
    def __init__(self):
        self.conn = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.conn.cursor(DictCursor)

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

class Neo4jWriter:
    def __init__(self):
        self.driver=GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self,label,properties):
        cypher=f'''
        UNWIND $properties as properties
        merge (:{label}{{id:properties.id,name:properties.name}})
        '''
        self.driver.execute_query(cypher,properties=properties)

    def add_properties(self,label,properties):
        cypher = f'''
                UNWIND $properties as properties
                match (n:{label}{{id:properties.id}})
                set n.origin_price=properties.origin_price,
                n.actual_price=properties.actual_price,
                n.teacher=properties.teacher
                '''

        def convert_decimal_to_float(data):
            """
            递归将字典/列表中的Decimal类型转换为float
            """
            if isinstance(data, dict):
                return {k: convert_decimal_to_float(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [convert_decimal_to_float(v) for v in data]
            elif isinstance(data, Decimal):
                # 转换为float（如果需要保留高精度，可改为str(data)）
                return float(data)
            else:
                return data
        properties = convert_decimal_to_float(properties)

        self.driver.execute_query(cypher, properties=properties)

    def write_relations(self,relation_type,start_label,end_label,relations):
        cypher=f'''
        UNWIND $relations as relations
        match (start:{start_label}{{id:relations.start_id}}),(end:{end_label}{{id:relations.end_id}})
        merge (start)-[:{relation_type}]->(end)
        '''
        self.driver.execute_query(cypher,relations=relations)

    def add_relations_properties(self,relation_type,start_label,end_label,relations_properties):
        cypher = f'''
                UNWIND $relations_properties as relations_properties
                match (start:{start_label}{{id:relations_properties.start_id}})-[r:{relation_type}]-(end:{end_label}{{id:relations_properties.end_id}})
                set r.is_correct =relations_properties.is_correct
                '''
        self.driver.execute_query(cypher, relations_properties=relations_properties)


if __name__ == '__main__':
    reader = MySqlReader()
    writer = Neo4jWriter()
    # 去除question的html标识
    cypher="match (n:Test_question) return n.id as id,n.name as name"
    driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
    h= html2text.HTML2Text()
    results=driver.execute_query(cypher).records
    properties=[{'id':result['id'],'name':h.handle(result['name'])}for result in results]
    cypher='''
            unwind $properties as properties
            match (n:Test_question{id:properties.id})
            set n.name=properties.name
        '''
    driver.execute_query(cypher,properties=properties)




    #
    # sql = '''
    #     select id,category_name as name
    #     from base_category_info
    #     '''
    # properties = reader.read(sql)
    # writer.write_nodes('Base_category', properties)
    #
    # sql = '''
    #         select id,subject_name as name
    #         from base_subject_info
    #         '''
    # properties = reader.read(sql)
    # writer.write_nodes('Base_subject', properties)
    #
    # sql = '''
    #             select a.id as start_id,b.id as end_id
    #             from base_subject_info as a join base_category_info as b on a.category_id = b.id
    #             '''
    # relations = reader.read(sql)
    # writer.write_relations('Belong','Base_subject','Base_category',relations)
