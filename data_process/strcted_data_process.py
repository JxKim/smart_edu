import mysql.connector
import pandas as pd
from neo4j import GraphDatabase
import re
from configuration.config import MYSQL_CONFIG,NEO4J_CONFIG
# --- 配置信息 ---

# Neo4j数据库连接配置
NEO4J_URI = NEO4J_CONFIG['uri']
NEO4J_USER = NEO4J_CONFIG['user']
NEO4J_PASSWORD = NEO4J_CONFIG['password']


# --- 1. 数据抽取 (从MySQL) ---
def get_data_from_mysql(query):
    """从MySQL获取数据并返回Pandas DataFrame"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        df = pd.read_sql(query, conn)
        print(f"成功从MySQL获取数据，查询: {query[:50]}..., 行数: {len(df)}")
        return df
    except mysql.connector.Error as err:
        print(f"MySQL错误: {err}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()


# --- 2. 图谱构建 (写入Neo4j) ---
class Neo4jImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        """执行一个Cypher查询"""
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def clear_database(self):
        """清空数据库，方便重新导入"""
        print("正在清空Neo4j数据库...")
        self.run_query("MATCH (n) DETACH DELETE n")
        print("数据库已清空。")

    def create_constraints(self):
        """为每个节点类型创建唯一性约束，非常重要！可以防止重复创建并加速查询"""
        print("正在创建唯一性约束...")
        constraints = [
            "CREATE CONSTRAINT c_category IF NOT EXISTS FOR (n:Category) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_subject IF NOT EXISTS FOR (n:Subject) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_course IF NOT EXISTS FOR (n:Course) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_teacher IF NOT EXISTS FOR (n:Teacher) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT c_chapter IF NOT EXISTS FOR (n:Chapter) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_video IF NOT EXISTS FOR (n:Video) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_paper IF NOT EXISTS FOR (n:Paper) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_question IF NOT EXISTS FOR (n:Question) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_knowledgepoint IF NOT EXISTS FOR (n:KnowledgePoint) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT c_user IF NOT EXISTS FOR (n:User) REQUIRE n.id IS UNIQUE",
        ]
        for constraint in constraints:
            self.run_query(constraint)
        print("约束创建完成。")

    def import_nodes(self, tx, label, data, id_col, prop_cols):
        """通用的节点导入函数 (修改后，不依赖APOC)"""
        # 如果没有其他属性需要设置，就只执行MERGE
        if not prop_cols:
            query = f"""
            UNWIND $rows AS row
            MERGE (n:{label} {{{id_col}: row.{id_col}}})
            """
        else:
            # 动态生成 SET 子句, 例如: SET n.name = row.name, n.price = row.price
            set_clauses = [f"n.{col} = row.{col}" for col in prop_cols]
            set_statement = ", ".join(set_clauses)

            query = f"""
            UNWIND $rows AS row
            MERGE (n:{label} {{{id_col}: row.{id_col}}})
            SET {set_statement}
            """
        tx.run(query, rows=data)
    def import_relationships(self, tx, start_label, end_label, start_id_col, end_id_col, rel_type, data):
        """通用关系导入函数"""
        query = f"""
        UNWIND $rows AS row
        MATCH (start:{start_label} {{{start_id_col}: row.start_id}})
        MATCH (end:{end_label} {{{end_id_col}: row.end_id}})
        MERGE (start)-[r:{rel_type}]->(end)
        """
        # 如果关系有属性，可以在这里添加 SET r += ...
        tx.run(query, rows=data)

    def import_teacher_relationships(self, tx, courses_df):
        """特殊处理教师关系，因为教师信息在course表里"""
        print("正在导入教师和授课关系...")
        query = """
        UNWIND $rows AS row
        MATCH (c:Course {id: row.course_id})
        // 教师名称可能包含多个，用逗号分隔
        WITH c, split(row.teacher_name, ',') AS teacher_names
        UNWIND teacher_names AS teacher_name
        MERGE (t:Teacher {name: trim(teacher_name)})
        MERGE (c)-[:TAUGHT_BY]->(t)
        """
        data = courses_df[['id', 'teacher']].rename(columns={'id': 'course_id', 'teacher': 'teacher_name'}).to_dict(
            'records')
        tx.run(query, rows=data)


def main():
    # --- 数据获取 ---
    categories_df = get_data_from_mysql(
        "SELECT id, category_name as name FROM base_category_info WHERE COALESCE(deleted, '0') != '1'")
    subjects_df = get_data_from_mysql(
        "SELECT id, subject_name as name, category_id FROM base_subject_info WHERE COALESCE(deleted, '0') != '1'")
    courses_df = get_data_from_mysql(
        "SELECT id, course_name as name, subject_id, teacher, origin_price as price FROM course_info WHERE COALESCE(deleted, '0') != '1'")
    chapters_df = get_data_from_mysql(
        "SELECT id, chapter_name as name, course_id, video_id FROM chapter_info WHERE COALESCE(deleted, '0') != '1'")
    videos_df = get_data_from_mysql(
        "SELECT id, video_name as name, during_sec as duration FROM video_info WHERE COALESCE(deleted, '0') != '1'")
    papers_df = get_data_from_mysql(
        "SELECT id, paper_title as title, course_id FROM test_paper WHERE COALESCE(deleted, '0') != '1'")
    questions_df = get_data_from_mysql(
        "SELECT id, question_txt as text, question_type as type FROM test_question_info WHERE COALESCE(deleted, '0') != '1'")
    users_df = get_data_from_mysql(
        "SELECT id, nick_name as name, birthday, gender FROM user_info")
    options_df = get_data_from_mysql(
        "SELECT id, option_txt as text, is_correct, question_id FROM test_question_option WHERE COALESCE(deleted, '0') != '1'"
    )
    # 【新增】将 is_correct 转换为布尔值，便于在Neo4j中存储和查询
    options_df['is_correct'] = options_df['is_correct'].apply(lambda x: x == '1')
    # --- 关联数据 ---
    print("正在从MySQL获取关系数据...")
    paper_question_rel = get_data_from_mysql(
        "SELECT paper_id, question_id, score FROM test_paper_question WHERE COALESCE(deleted, '0') != '1'")
    favor_rel_query = """
                      SELECT f.user_id, \
                             f.course_id, \
                             f.create_time, \
                             f.update_time
                      FROM favor_info f \
                               INNER JOIN \
                           user_info u ON f.user_id = u.id \
                               INNER JOIN \
                           course_info c ON f.course_id = c.id
                      WHERE COALESCE(f.deleted, '0') != '1' 
            AND COALESCE(c.deleted, '0') != '1' \
                      """
    favor_rel = get_data_from_mysql(favor_rel_query)
    watch_rel = get_data_from_mysql(
        "SELECT user_id, chapter_id, position_sec, update_time FROM user_chapter_progress WHERE COALESCE(deleted, '0') != '1'")
    answer_rel = get_data_from_mysql(
        "SELECT user_id, question_id, is_correct, create_time as submit_time FROM test_exam_question WHERE COALESCE(deleted, '0') != '1'")
    took_exam_rel = get_data_from_mysql(
        "SELECT user_id, paper_id, score, duration_sec, submit_time FROM test_exam WHERE COALESCE(deleted, '0') != '1'")
    paper_question_rel = get_data_from_mysql(
        "SELECT paper_id, question_id, score FROM test_paper_question")
    # 建立视频到章节的映射，方便后续观看关系导入
    chapter_to_video = chapters_df.set_index('id')['video_id'].to_dict()
    watch_rel['video_id'] = watch_rel['chapter_id'].map(chapter_to_video)
    watch_rel = watch_rel.dropna(subset=['video_id'])  # 移除那些没有对应视频的观看记录
    watch_rel['video_id'] = watch_rel['video_id'].astype(int)  # 确保ID是整数类型

    # --- Neo4j 数据导入 ---
    importer = Neo4jImporter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    importer.clear_database()
    importer.create_constraints()

    with importer.driver.session() as session:
        # --- 导入节点 ---
        print("开始导入节点...")
        session.execute_write(importer.import_nodes, "Category", categories_df.to_dict('records'), "id", ["name"])
        session.execute_write(importer.import_nodes, "Subject", subjects_df.to_dict('records'), "id", ["name"])
        session.execute_write(importer.import_nodes, "Course", courses_df.to_dict('records'), "id", ["name", "price"])
        session.execute_write(importer.import_nodes, "Chapter", chapters_df.to_dict('records'), "id", ["name"])
        session.execute_write(importer.import_nodes, "Video", videos_df.to_dict('records'), "id", ["name", "duration"])
        session.execute_write(importer.import_nodes, "Paper", papers_df.to_dict('records'), "id", ["title"])
        session.execute_write(importer.import_nodes, "Question", questions_df.to_dict('records'), "id",
                              ["text", "type"])
        session.execute_write(importer.import_nodes, "User", users_df.to_dict('records'), "id",
                              ["name", "birthday", "gender"])
        print("节点导入完成。")

        # --- 导入关系 ---
        print("开始导入关系...")
        # 结构性关系
        session.execute_write(importer.import_relationships, "Subject", "Category", "id", "id", "BELONGS_TO",
                              subjects_df[['id', 'category_id']].rename(
                                  columns={'id': 'start_id', 'category_id': 'end_id'}).to_dict('records'))
        session.execute_write(importer.import_relationships, "Course", "Subject", "id", "id", "BELONGS_TO",
                              courses_df[['id', 'subject_id']].rename(
                                  columns={'id': 'start_id', 'subject_id': 'end_id'}).to_dict('records'))
        session.execute_write(importer.import_teacher_relationships, courses_df)  # 特殊处理教师
        session.execute_write(importer.import_relationships, "Chapter", "Course", "id", "id", "PART_OF",
                              chapters_df[['id', 'course_id']].rename(
                                  columns={'id': 'start_id', 'course_id': 'end_id'}).to_dict('records'))
        session.execute_write(importer.import_relationships, "Chapter", "Video", "id", "id", "HAS_VIDEO",
                              chapters_df[['id', 'video_id']].rename(
                                  columns={'id': 'start_id', 'video_id': 'end_id'}).dropna().to_dict('records'))
        session.execute_write(importer.import_relationships, "Paper", "Course", "id", "id", "BELONGS_TO",
                              papers_df[['id', 'course_id']].rename(
                                  columns={'id': 'start_id', 'course_id': 'end_id'}).to_dict('records'))

        session.execute_write(importer.import_nodes, "Option", options_df.to_dict('records'), "id",
                              ["text", "is_correct"])
        session.execute_write(importer.import_relationships, "Question", "Option", "id", "id", "HAS_OPTION",
                              options_df[['question_id', 'id']].rename(
                                  columns={'question_id': 'start_id', 'id': 'end_id'}).to_dict('records'))
        # 关系带属性
        # (Paper)-[:HAS_QUESTION {score: ..}]->(Question)
        query_answered =  """
            UNWIND $rows AS row
            MATCH (p:Paper {id: row.paper_id})
            MATCH (q:Question {id: row.question_id})
            MERGE (p)-[r:HAS_QUESTION]->(q)
            SET r.score = row.score
        """
        session.run(query_answered, rows=paper_question_rel.to_dict('records'))

        # (User)-[:TOOK_EXAM {score: ..}]->(Paper)
        query_took_exam = """
        UNWIND $rows AS row
        MATCH (u:User {id: row.user_id})
        MATCH (p:Paper {id: row.paper_id})
        MERGE (u)-[r:TOOK_EXAM]->(p)
        SET r.score = row.score,
            r.duration_sec = row.duration_sec,
            r.submit_time = datetime(row.submit_time)
        """
        session.run(query_took_exam, rows=took_exam_rel.to_dict('records'))

        # 用户行为关系
        # (User)-[:FAVORS {created_time: ..}]->(Course)
        query_favors = """
        UNWIND $rows AS row
        MATCH (u:User {id: row.user_id})
        MATCH (c:Course {id: row.course_id})
        MERGE (u)-[r:FAVORS]->(c)
        SET r.created_time = datetime(row.create_time)
        """
        session.run(query_favors, rows=favor_rel.to_dict('records'))

        # (User)-[:WATCHED {position: ..}]->(Video)
        query_watched = """
        UNWIND $rows AS row
        MATCH (u:User {id: row.user_id})
        MATCH (v:Video {id: row.video_id})
        MERGE (u)-[r:WATCHED]->(v)
        SET r.position_sec = row.position_sec, r.updated_time = datetime(row.update_time)
        """
        session.run(query_watched, rows=watch_rel.to_dict('records'))

        # (User)-[:ANSWERED {is_correct: ..}]->(Question)
        query_answered = """
        UNWIND $rows AS row
        MATCH (u:User {id: row.user_id})
        MATCH (q:Question {id: row.question_id})
        MERGE (u)-[r:ANSWERED {submit_time: datetime(row.submit_time)}]->(q)
        SET r.is_correct = (row.is_correct = '1')
        """
        session.run(query_answered, rows=answer_rel.to_dict('records'))

        print("关系导入完成。")

    importer.close()
    print("\n知识图谱构建完成！")


if __name__ == "__main__":
    main()