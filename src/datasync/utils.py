from typing import Union
import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor

from configurations import config


class Utils:
    def __init__(self):
        self.connection = pymysql.connect(**config.SQL_CONFIG)
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    # 读取MySQL数据
    def read_sql(self, table_name):
        with self.connection.cursor(DictCursor) as cursor:
            # %s占位符 不能用于表名的占位
            # language=sql
            sql = f"""
                select 
                * 
                from 
                {table_name}
            """
            cursor.execute(query=sql)
            result = cursor.fetchall()
            return result

    # 向Neo4j写入节点
    def write_node(self, table_name: str, label: str, messages: Union[str, list],result):
        nodes = []
        # 结构是这样的[{},{},{}]
        for item in result:
            # 创建一个字典,字典的key是外部传进来的字段，字典的值,是根据这个字段在数据库中拿的值
            properties_dict = {message: item[message] for message in messages}
            nodes.append(properties_dict)
        # 节点的标签不能使用$占位,节点的属性可以使用$占位
        # merge负责去重，需要唯一的id,+=负责批量复制，虽然会覆盖前面的id,但是都是一个 所以无所谓
        cypher = f"""
            Unwind $nodes as node 
            merge (n:{label} {{id:node.id}})
            SET n += node 
        """
        self.driver.execute_query(query_=cypher, parameters_={"nodes": nodes})
        print(f"写入{table_name}节点成功!")

    # 向Neo4j写入关系
    def write_relation(self, table_name: str,start_label,end_label,relation_type,start_id,end_id):
        with self.connection.cursor(DictCursor) as cursor:
            # %s占位符 不能用于表名的占位
            # language=sql
            sql = f"""
                select 
                {start_id} start_id,
                {end_id} end_id
                from 
                {table_name}
            """
            cursor.execute(query=sql)
            result = cursor.fetchall()

        cypher=f"""
            unwind $result as item
            match (start:{start_label} {{id:item.start_id}}),(end:{end_label} {{id:item.end_id}} ) 
            merge (start)-[:{relation_type}]->(end)
        """

        self.driver.execute_query(query_=cypher, parameters_={"result": result})
        print(f"{start_label}到{end_label}的{relation_type}关系建立完成!")

    # 删除所有索引
    @staticmethod
    def delete_indexes():
        driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
        with driver.session() as session:
            # 获取所有索引
            indexes = session.run("SHOW INDEXES YIELD name")
            for record in indexes:
                index_name = record["name"]
                session.run(f"DROP INDEX `{index_name}`")
                print(f"✅ 删除索引: {index_name}")


if __name__ == '__main__':
    # 测试通过
    util=Utils()
    util.delete_indexes()
    # util.write_relation(table_name="user_chapter_progress",
    #                     start_label="user",
    #                     end_label="chapter_progress",
    #                     relation_type="WATCHED",
    #                     start_id="id",
    #                     end_id="user_id")


