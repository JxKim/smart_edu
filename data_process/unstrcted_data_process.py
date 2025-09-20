import pandas as pd
from neo4j import GraphDatabase
from configuration.config import NEO4J_CONFIG

# --- 配置 ---
CSV_FILE_PATH = 'course_knowledge_tags.csv'
uri = NEO4J_CONFIG['uri']
user = NEO4J_CONFIG['user']
password = NEO4J_CONFIG['password']

class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def setup_constraints(self):
        """为 KnowledgePoint 的 name 属性创建唯一性约束，非常重要！"""
        print("正在设置 KnowledgePoint 名称唯一性约束...")
        query = "CREATE CONSTRAINT kp_name_unique IF NOT EXISTS FOR (kp:KnowledgePoint) REQUIRE kp.name IS UNIQUE"
        with self.driver.session() as session:
            session.run(query)
        print("约束设置完成。")

    def load_tags_from_csv(self, file_path):
        df = pd.read_csv(file_path)
        # 将CSV中的字符串列表 '["tag1", "tag2"]' 转换回真正的Python列表
        # 使用 ast.literal_eval 是安全的做法
        from ast import literal_eval
        df['knowledge_tags'] = df['knowledge_tags'].apply(literal_eval)

        # 转换成Neo4j查询需要的格式: [{'course_id': 1, 'tags': ['t1', 't2']}, ...]
        records = df[['id', 'knowledge_tags']].rename(columns={'id': 'course_id', 'knowledge_tags': 'tags'}).to_dict(
            'records')

        # 使用 UNWIND 高效批量写入
        cypher_query = """
        UNWIND $rows AS row
        MATCH (c:Course {id: row.course_id})
        // 对当前课程的每一个标签进行操作
        UNWIND row.tags AS tag_name
        // 使用 MERGE 创建 KnowledgePoint 节点，如果不存在的话
        MERGE (kp:KnowledgePoint {name: trim(tag_name)})
        // 使用 MERGE 创建课程与知识点之间的关系，如果不存在的话
        MERGE (c)-[:COVERS_KNOWLEDGE]->(kp)
        """

        with self.driver.session() as session:
            print(f"正在批量写入 {len(records)} 条课程的知识点关系...")
            session.run(cypher_query, rows=records)
            print("写入完成！")


def main_load():
    loader = Neo4jLoader(uri,user,password)
    # 1. 确保约束存在
    loader.setup_constraints()
    # 2. 从CSV加载并写入图库
    loader.load_tags_from_csv(CSV_FILE_PATH)
    loader.close()


if __name__ == "__main__":
    # 运行第一个脚本来生成CSV
    # process_courses()

    # 然后运行这个函数来加载数据到Neo4j
    main_load()