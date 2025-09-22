import pymysql
from neo4j import GraphDatabase
from pymysql.cursors import DictCursor
from configuration import config

class MysqlReader:
    def __init__(self):
        self.connection=pymysql.connect(**config.MYSQL_CONFIG)
        #用pymysql连接数据库，初始化时就连接
        self.cursor=self.connection.cursor(DictCursor)
        #这里用cursor处理查询语句
    def read(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

class Neo4jWriter:
    def __init__(self):
        self.driver=GraphDatabase.driver(**config.NEO4J_CONFIG)
        #这里用GraphDatabase的driver连接neo4j
    def write_nodes(self,label:str,properties:list[dict]):
        cypher=f"""
        UNWIND $batch AS item
        MERGE (n:{label}{{id:item.id}})
        SET n += item
        """
        self.driver.execute_query(cypher,batch=properties)

    def write_nodes_noid(self,label:str,properties:list[dict]):
        cypher=f"""
        UNWIND $batch AS item
        MERGE (n:{label}{{name:item.teacher}})
        """
        self.driver.execute_query(cypher,batch=properties)

    def write_relations(self,type:str,start_label,end_label,relations:list[dict]):
        cypher=f"""
            UNWIND $batch AS item
            MATCH (start:{start_label}{{id:item.start_id}}),(end:{end_label}{{id:item.end_id}})
            MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher,batch=relations)
    def write_relations_teacher(self,type:str,start_label,end_label,relations:list[dict]):
        cypher=f"""
            UNWIND $batch AS item
            MATCH (start:{start_label}{{name:item.name}}),(end:{end_label}{{id:item.end_id}})
            MERGE (start)-[:{type}]->(end)
        """
        self.driver.execute_query(cypher,batch=relations)

    def write_relations_knowledge_PREREQUISITE(self):
        cypher="""
        UNWIND[
            {
        from:'HTML', to: 'JavaScript'},
        {
        from:'JavaScript', to: 'React'},
        {
        from:'JavaScript', to: 'Vue'},
        {
        from:'Servlet', to: 'JSP'},
        {
        from:'JSP', to: 'Spring框架'},
        {
        from:'Spring框架', to: 'Tomcat'},
        {
        from:'JDBC', to: 'EJB'},
        {
        from:'大数据', to: 'Hadoop'},
        {
        from:'Hadoop', to: 'Spark'},
        {
        from:'数据仓库', to: '数据湖'},
        {
        from:'数据湖', to: 'ETL过程'},
        {
        from:'数据结构', to: '算法设计'},
        {
        from:'算法设计', to: '编译原理'},
        {
        from:'面向对象', to: '设计模式'},
        {
        from:'多线程', to: '并发编程'},
        {
        from:'分布式系统', to: '并行计算'},
        {
        from:'网络协议', to: 'TCP/IP协议'},
        {
        from:'TCP/IP协议', to: 'HTTP协议'},
        {
        from:'网络安全', to: '分布式系统'}
        ] AS rel
        MATCH(a: knowledge_point
        {point_txt: rel.
        from})
        MATCH(b: knowledge_point
        {point_txt: rel.to})
        MERGE(a) - [: PREREQUISITE]->(b)
        """
        self.driver.execute_query(cypher)
        print('knowledge_PREREQUISITE关系建立成功')

    def write_relations_knowledge_BELONG(self):
        cypher="""
        UNWIND [
  {parent:'数据结构', child:'链表结构'},
  {parent:'数据结构', child:'树结构'},
  {parent:'数据结构', child:'图结构'},
  {parent:'算法设计', child:'排序算法'},
  {parent:'算法设计', child:'搜索算法'},
  {parent:'算法设计', child:'动态规划'},
  {parent:'并发编程', child:'多线程'},
  {parent:'设计模式', child:'单例模式'},
  {parent:'设计模式', child:'工厂模式'},
  {parent:'设计模式', child:'观察者模式'},
  {parent:'设计模式', child:'策略模式'},
  {parent:'设计模式', child:'适配器模式'}
] AS rel
MATCH (p:knowledge_point {point_txt: rel.parent})
MATCH (c:knowledge_point {point_txt: rel.child})
MERGE (p)-[:BELONG]->(c)
        """
        self.driver.execute_query(cypher)
        print('knowledge_BELONG关系建立成功')

    def write_relations_knowledge_RELATED(self):
        cypher = """
UNWIND [
  {a:'Hadoop', b:'Spark'},
  {a:'数据挖掘', b:'数据可视化'},
  {a:'React', b:'Vue'},
  {a:'分布式系统', b:'NoSQL数据库'},
  {a:'Spring框架', b:'Hibernate'},
  {a:'Git系统', b:'版本控制'},
  {a:'Docker技术', b:'Kubernetes'}
] AS rel
MATCH (a:knowledge_point {point_txt: rel.a})
MATCH (b:knowledge_point {point_txt: rel.b})
MERGE (a)-[:RELATED]->(b)
MERGE (b)-[:RELATED]->(a)
        """
        self.driver.execute_query(cypher)
        print('knowledge_RELATED关系建立成功')


if __name__ == '__main__':
    mysql_reader = MysqlReader()
    neo4j_writer = Neo4jWriter()
    # #读取base_category_info的数据
    # sql="""
    # select id,category_name as name
    # from base_category_info
    # """
    # category1=mysql_reader.read(sql)
    # print(category1)
    # neo4j_writer.write_nodes('base_category_info',category1)
    #
    # #读取base_subject_info的数据
    # sql="""
    # select id,subject_name as name
    # from base_subject_info
    # """
    # category2=mysql_reader.read(sql)
    # print(category2)
    # neo4j_writer.write_nodes('base_subject_info',category2)
    #
    # sql="""
    # select id as start_id,category_id as end_id
    # from base_subject_info"""
    # category1_to_category2=mysql_reader.read(sql)
    # print(category1_to_category2)
    # neo4j_writer.write_relations('Belong','base_subject_info','base_category_info',category1_to_category2)

    #读取base_category_info的数据
    # sql="""
    # select *
    # from base_province
    # """
    # category1=mysql_reader.read(sql)
    # print(category1)
    # neo4j_writer.write_nodes('base_province',category1)


    #knowledge_PREREQUISITE关系
    # neo4j_writer.write_relations_knowledge_PREREQUISITE()
    neo4j_writer.write_relations_knowledge_BELONG()
    neo4j_writer.write_relations_knowledge_RELATED()