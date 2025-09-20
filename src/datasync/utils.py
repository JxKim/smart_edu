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

    def write_nodes(self, label: str, properties: list[dict], id_field: str = "id", name_field: str = None):
        """
        写入节点到Neo4j，支持不同表的属性结构，处理Decimal类型和空值

        :param label: 节点标签
        :param properties: 节点属性列表
        :param id_field: 唯一标识字段名，默认为"id"
        :param name_field: 名称字段名，可为None（表示不使用name字段进行MERGE）
        """
        from decimal import Decimal  # 导入Decimal类型用于判断

        # 处理空值和类型转换
        processed_props = []
        for prop in properties:
            processed = {}
            for key, value in prop.items():
                # 处理Decimal类型，转换为float
                if isinstance(value, Decimal):
                    processed[key] = float(value)
                # 处理空值
                elif value is None:
                    processed[key] = "NULL"
                # 其他类型保持不变
                else:
                    processed[key] = value
            processed_props.append(processed)

        # 构建MERGE条件
        if name_field and processed_props and name_field in processed_props[0]:
            # 既有id又有name字段时，使用两者作为合并条件
            merge_clause = f"MERGE (n:{label} {{{id_field}:item.{id_field},{name_field}:item.{name_field}}})"
        else:
            # 只有id字段时，仅使用id作为合并条件
            merge_clause = f"MERGE (n:{label} {{{id_field}:item.{id_field}}})"

        # 构建SET子句，设置所有属性
        set_clause = "SET n += item"

        # 完整Cypher语句
        cypher = f"""
            UNWIND $batch AS item
            {merge_clause}
            {set_clause}
        """

        self.driver.execute_query(cypher, batch=processed_props)

    def write_relations(self, type: str, start_label, end_label, relations: list[dict],
                        start_id_field: str = "id", end_id_field: str = "id"):
        """
        写入关系到Neo4j，支持类型转换和自定义ID字段

        :param type: 关系类型
        :param start_label: 起始节点标签
        :param end_label: 目标节点标签
        :param relations: 关系数据列表，包含start_id和end_id
        :param start_id_field: 起始节点的ID字段名，默认为"id"
        :param end_id_field: 目标节点的ID字段名，默认为"id"
        """
        from decimal import Decimal  # 处理Decimal类型

        # 处理关系数据中的类型转换和空值
        processed_relations = []
        for rel in relations:
            processed = {}
            for key, value in rel.items():
                # 处理Decimal类型
                if isinstance(value, Decimal):
                    processed[key] = float(value)
                # 处理空值
                elif value is None:
                    processed[key] = "NULL"
                # 处理ID字段可能的类型不一致问题（确保为整数）
                elif key in ["start_id", "end_id"]:
                    try:
                        processed[key] = int(value)
                    except (ValueError, TypeError):
                        processed[key] = value  # 保留原始值，避免转换失败
                else:
                    processed[key] = value
            processed_relations.append(processed)

        # 构建Cypher语句，支持自定义ID字段
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{{start_id_field}:item.start_id}})
                MATCH (end:{end_label} {{{end_id_field}:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """

        self.driver.execute_query(cypher, batch=processed_relations)
        result = self.driver.execute_query(cypher, batch=processed_relations)

        # 打印执行结果
        print(f"关系 {type} 执行结果: {result.summary.counters}")
        # 检查是否有创建的关系
        if result.summary.counters.relationships_created == 0 and len(processed_relations) > 0:
            print(f"警告: 未创建任何 {type} 关系，可能节点不存在或ID不匹配")


if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    # 读取Category1的数据
    sql = """
          select id,
                 name
          from base_category1
          """
    category1 = mysql_reader.read(sql)
    print(category1)
    # 写入Category1的数据
    neo4j_writer.write_nodes("Category1", category1)

    # 读取Category2的数据
    sql = """
          select id,
                 name
          from base_category2
          """
    category2 = mysql_reader.read(sql)
    # 写入Category2的数据
    neo4j_writer.write_nodes("Category2", category2)

    # 读取category1和category2的关系
    sql = """
          select id           start_id,
                 category1_id end_id
          from base_category2
          """
    category1_to_category2 = mysql_reader.read(sql)
    print(category1_to_category2)

    # 写入category1和category2的关系
    neo4j_writer.write_relations("Belong", "Category2", "Category1", category1_to_category2)
