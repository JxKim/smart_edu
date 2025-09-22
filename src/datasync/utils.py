from tarfile import NUL
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

    def write_nodes(self, label: str, properties: list[dict], merge_keys: list[str] = ["id"]):
        """
        label: 节点标签
        properties: 节点属性列表，每个元素是字典
        merge_keys: 用于 MERGE 匹配节点的字段列表
        """
        # 处理每条记录，去掉 merge_keys 为 None 的条目
        filtered_props = []
        for item in properties:
            # 保留 merge_keys 不为 None 的字段
            valid_merge = {k: item[k] for k in merge_keys if item.get(k) is not None}
            if not valid_merge:
                # 如果所有 merge_keys 都为 None，就跳过
                continue
            # 保存原属性和过滤后的 merge_keys
            filtered_props.append({"item": item, "merge": valid_merge})

        cypher = f"""
            UNWIND $batch AS row
            MERGE (n:{label}) 
            ON CREATE SET n += row.item
            ON MATCH SET n += row.item
        """
        # 注意：merge 条件在这里用 row.merge 生成动态 MERGE
        # Neo4j Python 驱动需要用参数方式传递
        for row in filtered_props:
            merge_str = ",".join([f"{k}: {v!r}" for k, v in row["merge"].items()])
            cypher = f"MERGE (n:{label} {{{merge_str}}}) SET n += {row['item']}"

        self.driver.execute_query(cypher, batch=filtered_props)


    def write_relations(self, type: str, start_label, end_label, relations: list[dict]):
        cypher = f"""
                UNWIND $batch AS item
                MATCH (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}})
                MERGE (start)-[:{type}]->(end)
            """
        self.driver.execute_query(cypher, batch=relations)
    
    def write_self_relations(self, type: str, label, relations: list[dict]):
        cypher = f"""
            UNWIND $batch AS item
            MATCH (start:{label} {{id: item.start_id}}),
                (end:{label} {{name: item.end_name}})
            MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher, batch=relations)
    

    def write_nodes2(self, label: str, properties: list[dict]):
       
        # Cypher 语句：UNWIND 会把列表展开成多行，每行赋给 item
        # item 结构是一个 dict（map），可用 item.id, item.name 访问
        # MERGE 确保节点存在，不会重复创建
        cypher = f"""
            UNWIND $batch AS item
            MERGE (n:{label} {{id:item.id, name:item.name}})
            """
        # 执行时传入参数 batch=properties
        # properties 会自动绑定到 $batch 变量
        self.driver.execute_query(cypher, batch=properties)


