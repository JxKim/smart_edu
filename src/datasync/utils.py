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
    # driver只用定义一次，使用多次。使用过程中不要多次定义
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self, label: str, properties: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id, name:item.name}})
            """
        self.driver.execute_query(cypher, batch=properties)

    def write_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)



if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()

    # "查询分类下有哪些学科"
    # 建立分类节点
    sql = """
        select
            id, category_name as name
        from
            base_category_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_nodes("category", properties=result)
    # 建立学科节点
    sql = """
        select
            id, subject_name as name
        from
            base_subject_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_nodes("subject", properties=result)
    # 建立关系：学科属于分类，学科->分类
    sql = """
            select
                id      as  start_id,
                category_id as end_id
            from
                base_subject_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_relations("belong", start_label="subject", end_label="category", relations=result)

    # "查询学科下有哪些课程"
    # 建立课程节点，从course_info表的course_name和id提取id和name
    sql = """
        select
            id,  course_name as name
        from
            course_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_nodes("course", properties=result)

    # 建立关系, 课程属于分类，课程->分类
    sql = """
            select
                id as start_id,
                subject_id as end_id
            from
                course_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_relations("belong", start_label="course", end_label="subject", relations=result)

    # "查询课程下有哪些章节"
    # 建立章节节点
    sql = """
        select 
            id,
            chapter_name as name
        from
            chapter_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_nodes("cheapter", properties=result)
    # 建立关系：章节属于课程，章节->课程
    sql = """
            select
                id as start_id,
                course_id as end_id
            from
                chapter_info
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_relations("belong", start_label="chapter", end_label="course", relations=result)

    '''
    "查询课程教师",
    "查询课程对应知识点",
    "查询章节对应知识点",
    "查询与某知识点相关的课程",
    "查询与某知识点相关的章节",
    "查询与某知识点相关的试题",
    "查询知识点的先修知识点",
    "查询知识点被哪个知识点包含",
    "查询知识点包含哪些知识点",
    "查询知识点有哪些相关知识点",
    "查询学生做错的试题",
    "查询学生看过的视频",
    '''

    # 查询课程下有哪些试卷
    sql = """
            select
                id, 
                paper_title as name
            from 
                test_paper
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_nodes("paper", properties=result)
    # 建立关系：试卷属于课程，试卷->课程
    sql = """
            select
                id as start_id,
                course_id as end_id
            from
                test_paper
    """
    result = mysql_reader.read(sql)
    neo4j_writer.write_relations("belong", start_label="paper", end_label="course", relations=result)

    # 查询课程价格
    # 给课程添加价格属性
    sql = """
        select
            id, 
            actual_price as price
        from
            course_info
    """
    result = mysql_reader.read(sql)
    for row in result:
        if 'price' in row and row['price'] is not None:
            row['price'] = float(row['price'])
    # 批量更新课程节点
    cypher = """
        unwind $batch as item
        match (n:course {id: item.id} )
        set n.price = item.price 
    """
    neo4j_writer.driver.execute_query(cypher, batch=result)




