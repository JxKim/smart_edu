# database_connector.py
import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor
from typing import List , Dict , Any
import logging

# 配置日志
logging.basicConfig ( level=logging.INFO )
logger = logging.getLogger ( __name__ )


class MysqlReader :
    def __init__ ( self , mysql_config ) :
        self.connection = pymysql.connect ( **mysql_config )
        self.cursor = self.connection.cursor ( DictCursor )

    def read ( self , sql ) :
        try :
            self.cursor.execute ( sql )
            return self.cursor.fetchall ( )
        except Exception as e :
            logger.error ( f"执行SQL查询失败: {e}" )
            return []

    def close ( self ) :
        self.cursor.close ( )
        self.connection.close ( )


class Neo4jWriter :
    def __init__ ( self , neo4j_config ) :
        self.driver = GraphDatabase.driver ( **neo4j_config )

    def write_nodes ( self , label: str , key_property: str , properties: List[Dict] ) :
        """写入节点，使用指定的键属性作为MERGE条件"""
        if not properties :
            logger.warning ( f"没有属性数据可写入节点 {label}" )
            return

        # 动态构建SET子句，更新所有属性
        other_properties = [k for k in properties[0].keys ( ) if k != key_property]
        set_clause = ", ".join ( [f"n.{k} = item.{k}" for k in other_properties] )

        # 处理没有其他属性的情况
        if set_clause :
            cypher = f"""
                UNWIND $batch AS item
                MERGE (n:{label} {{{key_property}: item.{key_property}}})
                SET {set_clause}
            """
        else :
            # 没有其他属性需要更新，只需要MERGE即可
            cypher = f"""
                UNWIND $batch AS item
                MERGE (n:{label} {{{key_property}: item.{key_property}}})
            """

        try :
            result = self.driver.execute_query ( cypher , batch=properties )
            logger.info ( f"成功写入 {len ( properties )} 个 {label} 节点" )
            return result
        except Exception as e :
            logger.error ( f"写入节点失败: {e}" )

    def write_relations ( self , rel_type: str , start_label: str , end_label: str ,
                          start_key: str , end_key: str , relations: List[Dict] ,
                          rel_properties: List[str] = None ) :
        """写入关系，可以包含关系属性"""
        if not relations :
            logger.warning ( f"没有关系数据可写入关系 {rel_type}" )
            return

        # 如果有关系属性，构建SET子句
        set_clause = ""
        if rel_properties :
            set_clause = "SET " + ", ".join ( [f"r.{prop} = item.{prop}" for prop in rel_properties] )

        cypher = f"""
            UNWIND $batch AS item
            MATCH (start:{start_label} {{{start_key}: item.start_id}})
            MATCH (end:{end_label} {{{end_key}: item.end_id}})
            MERGE (start)-[r:{rel_type}]->(end)
            {set_clause}
        """

        try :
            result = self.driver.execute_query ( cypher , batch=relations )
            logger.info ( f"成功写入 {len ( relations )} 个 {rel_type} 关系" )
            return result
        except Exception as e :
            logger.error ( f"写入关系失败: {e}" )

    def close ( self ) :
        self.driver.close ( )

