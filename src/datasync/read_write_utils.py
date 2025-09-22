from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config


class MysqlReader:
    def __init__(self):
        import pymysql
        self.connection = pymysql.connect(**config.SQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)

    def read(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall() #🔥返回字典形式

# class Neo4jWriter:
#     def __init__(self):
#         self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
#
#     def write_nodes(self,label:str,properties:list[dict]):
#         cypher = f'''
#             UNWIND $batch AS item
#             MERGE (n:{label} {{id:item.id,name:item.name}})
#             '''
#         self.driver.execute_query(cypher,batch=properties)
#
#     def write_relations(self,type:str,start_label,end_label,relations:list[dict]):
#         cypher = f'''UNWIND $batch AS item
#                      MATCH (start:{start_label} {{id:item.start_id}}), (end:{end_label} {{id:item.end_id}})
#                      MERGE (start)-[r:{type}]->(end)
#         '''
#         self.driver.execute_query(cypher,batch=relations)
class Neo4jWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
        self.driver.execute_query('MATCH (n) DETACH DELETE n') #❗强制删除所有节点

    def write_nodes(self, label: str, properties: list[dict]):
        # 动态生成属性匹配的 Cypher 语句
        property_keys = set()
        for prop in properties:
            property_keys.update(prop.keys())

        property_keys = sorted(property_keys)  # 确保顺序一致 ['id', 'name']
        property_placeholders = ", ".join(f"{key}: item.{key}" for key in property_keys)


        cypher = f'''
            UNWIND $batch AS item
            MERGE (n:{label} {{ {property_placeholders} }})
        '''
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, type: str, start_label: str, end_label: str, relations: list[dict]):
        cypher = f'''
            UNWIND $batch AS item
            MATCH (start:{start_label} {{id:item.start_id}}), (end:{end_label} {{id:item.end_id}})
            MERGE (start)-[r:{type}]->(end)
        '''
        print('👉cypher语句:',cypher)
        self.driver.execute_query(cypher, batch=relations)

    def write_relations_with_propert(self, type: str, start_label: str, end_label: str, relations: list[dict]):
        # 动态生成关系属性的 Cypher 语句
        relation_keys = set()
        for relation in relations: #⭐<class 'dict_keys'> - {'',''}
            relation_keys.update(relation.keys() - {'start_id', 'end_id'})  # 排除掉start_id和end_id，只保留关系属性

        relation_keys = sorted(relation_keys)  # 确保顺序一致
        relation_placeholders = ", ".join(f"{key}: item.{key}" for key in relation_keys)

        cypher = f'''
            UNWIND $batch AS item
            MATCH (start:{start_label} {{id:item.start_id}}), (end:{end_label} {{id:item.end_id}})
            MERGE (start)-[r:{type} {{ {relation_placeholders} }}]->(end)
        '''
        print('👉cypher语句:',cypher)
        self.driver.execute_query(cypher, batch=relations)

'''
①假设你有以下节点数据：
nodes = [
    {"id": "1", "name": "Alice", "age": 30},
    {"id": "2", "name": "Bob", "age": 25},
    {"id": "3", "name": "Charlie", "city": "New York"}
]
②调用 write_nodes 方法：
writer = Neo4jWriter()
writer.write_nodes("Person", nodes)
③生成的 Cypher 语句将是：
UNWIND $batch AS item
MERGE (n:Person {id: item.id, name: item.name, age: item.age, city: item.city})
这样，无论节点的属性是什么，都可以正确地写入到 Neo4j 数据库中。
'''

if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    # mysql_reader.read('select * from user')
    # neo4j_writer.write_nodes('User',[{'id':1,'name':'张三'}])

    # sql1 = """
    # select id,category_name as name from base_category_info
    #
    # """

    # properties = mysql_reader.read(sql1)
    # neo4j_writer.write_nodes('CourseCategory', properties)
    #
    # sql2 = """
    #         select id,subject_name as name from base_subject_info
    #         """
    # properties = mysql_reader.read(sql2)
    # neo4j_writer.write_nodes('SubjectId', properties)
    #
    # # 学科 belong 课程分类
    # sql = '''
    # select id as start_id,category_id as end_id
    # from base_subject_info
    # '''
    # relations = mysql_reader.read(sql)
    # # import pdb;pdb.set_trace()
    # neo4j_writer.write_relations('BELONG', 'SubjectId', 'CourseCategory', relations)
    # print('🍉学科 --belong--> 课程分类')

    #课程节点
    sql = """
    select id,course_name as name from course_info
    """
    properties = mysql_reader.read(sql)
    neo4j_writer.write_nodes('Course', properties)

    #学生节点
    sql = '''
    select id ,real_name,birthday ,gender
    from user_info
    '''
    properties = mysql_reader.read(sql)
    for prop in properties:
        gender = prop.get('gender')
        # 处理性别
        if gender == 'M':
            prop['gender'] = '男'
        elif gender == 'F':
            prop['gender'] = '女'
        else:
            prop['gender'] = '未知'
    neo4j_writer.write_nodes('Student', properties)
    # # 学生 favor 课程               create_time as time #❗
    # sql = '''select user_id as start_id, course_id as end_id
    # from user_chapter_progress
    # '''
    #
    # relations = mysql_reader.read(sql)
    # # import pdb;pdb.set_trace()
    # print(relations[:2])
    # neo4j_writer.write_relations('FAVOR', 'Student', 'Course', relations)
    # print('🍉学生 --favor--> 课程')
#======================================================================================================
    sql = '''select user_id as start_id, course_id as end_id , create_time as favor_time
    from user_chapter_progress
    '''

    relations = mysql_reader.read(sql)
    # import pdb;pdb.set_trace()
    print(relations[:2])
    neo4j_writer.write_relations_with_propert('FAVOR', 'Student', 'Course', relations)
    print('🍉学生 --favor--> 课程')












