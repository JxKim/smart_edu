import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configuration import config


class MysqlReader:
    """
    Mysql数据库读取类
    """

    def __init__(self):
        self.connection = pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor = self.connection.cursor(DictCursor)

    def read(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class Neo4jWriter:
    """
    Neo4j数据库写入类
    """

    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def write_nodes(self, label, properties):
        """
        写入节点
        :param label: 节点标签
        :param properties: 节点属性列表
        :return:
        """
        cypher = f"""
                UNWIND $properties as property
                MERGE (n:{label}{{id: property.id, name: property.name}})
        """
        self.driver.execute_query(cypher, properties=properties)

    def write_nodes_name(self, label, properties):
        """
        写入节点
        :param label: 节点标签
        :param properties: 节点属性列表
        :return:
        """
        cypher = f"""
                UNWIND $properties as property
                MERGE (n:{label}{{name: property.name}})
        """
        self.driver.execute_query(cypher, properties=properties)

    def write_nodes_price(self, label, properties):
        """
        写入节点
        :param label: 节点标签
        :param properties: 节点属性列表
        :return:
        """
        cypher = f"""
                UNWIND $properties as property
                MERGE (n:{label}{{price: property.price}})
        """
        self.driver.execute_query(cypher, properties=properties)

    def write_nodes_student(self, label, properties):
        """
        写入节点
        :param label: 节点标签
        :param properties: 节点属性列表
        :return:
        """
        cypher = f"""
                UNWIND $properties as property
                MERGE (n:{label}{{uid: property.uid, 生日: property.birthday, 性别: property.gender}})
        """
        self.driver.execute_query(cypher, properties=properties)

    def write_relations(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{id:relation.start_id}}),(end:{end_label} {{id:relation.end_id}})
                MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, relations=relations)

    def write_relations_name(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{id:relation.start_id}}),(end:{end_label} {{name:relation.end_name}})
                MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, relations=relations)

    def write_relations_price(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{id:relation.start_id}}),(end:{end_label} {{price:relation.end_price}})
                MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, relations=relations)

    def write_relations_favor(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{uid:relation.start_id}}),(end:{end_label} {{id:relation.end_id}})
                MERGE (start)-[:{type} {{时间:relation.time}}]->(end)
        """
        self.driver.execute_query(cypher, relations=relations)

    def write_relations_correct(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{uid:relation.start_id}}),(end:{end_label} {{id:relation.end_id}})
                MERGE (start)-[:{type} {{是否正确:relation.is_correct}}]->(end)
        """
        return self.driver.execute_query(cypher, relations=relations)

    def write_relations_watch(self, type, start_label, end_label, relations):
        """
        写入关系
        :param type: 关系类型
        :param start_label: 开始标签
        :param end_label: 结束标签
        :param relations: 关系id列表
        :return:
        """
        cypher = f"""
                UNWIND $relations as relation
                MATCH (start:{start_label} {{uid:relation.start_id}}),(end:{end_label} {{id:relation.end_id}})
                MERGE (start)-[:{type} {{时间:relation.position_sec, 最后观看时间:relation.time}}]->(end)
        """
        self.driver.execute_query(cypher, relations=relations)


if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    sql = """
        select
            id, 
            category_name name
        from base_category_info
    """
    category = mysql_reader.read(sql)
    print(category)
    neo4j_writer.write_nodes("分类", category)
