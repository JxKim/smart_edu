from neo4j import GraphDatabase

from src.configuration import config
from src.datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
    def sync_base_category_info(self):
        sql="""
            select
                id,
                category_name name
            from base_category_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('BaseCategoryInfo', properties)

    def sync_base_subject_info(self):
        sql="""
            select
                id,
                subject_name name
            from base_subject_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('BaseSubjectInfo', properties)

    def sync_course_info(self):
        sql="""
            select
                id,
                course_name name
            from course_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('CourseInfo', properties)

    def sync_teacher_name(self):
        sql="""
            select
                id,
                teacher_name name
            from teacher_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('TeacherInfo', properties)

    def sync_price(self):
        sql="""
            select
                id,
                CAST(origin_price AS FLOAT ) AS name
            from course_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Price', properties)

    def sync_chapter_info(self):
        sql="""
            select
                id,
                chapter_name name
            from chapter_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('ChapterInfo', properties)

    def sync_video_info(self):
        sql="""
            select
                id,
                video_name name
            from video_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('VideoInfo', properties)

    def sync_test_paper(self):
        sql="""
            select
                id,
                paper_title name
            from test_paper
        """
        properties =self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('TestPaper', properties)

    def sync_test_question_info(self):
        sql="""
            select
                id,
                question_txt name
            from test_question_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('TestQuestionInfo', properties)

    def sync_course_info_to_base_subject_info(self):
        sql="""
            select
                id start_id,
                subject_id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'CourseInfo', 'BaseSubjectInfo', relations)

    def sync_base_subject_info_to_base_category_info(self):
        sql="""
            select
                id start_id,
                category_id end_id
            from base_subject_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'BaseSubjectInfo', 'BaseCategoryInfo', relations)

    def sync_vido_info_to_chapter_info(self):
        sql="""
            select
                id start_id,
                chapter_id end_id
            from video_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'VideoInfo', 'ChapterInfo', relations)

    def sync_chapter_info_to_course_info(self):
        sql="""
            select
                id start_id,
                course_id end_id
            from chapter_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'ChapterInfo', 'CourseInfo', relations)

    def sync_test_question_info_to_test_paper(self):
        sql="""
            select
                question_id start_id,
                paper_id end_id
            from test_paper_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'TestQuestionInfo', 'TestPaper', relations)

    def sync_test_paper_to_course_info(self):
        sql="""
            select
                id start_id,
                course_id end_id
            from test_paper
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'TestPaper', 'CourseInfo', relations)

    def sync_course_info_to_teacher_info(self):
        sql="""
            select
                id start_id,
                teacher_id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('HAVE', 'CourseInfo', 'TeacherInfo', relations)

    def sync_course_info_to_price(self):
       sql="""
            select
                id start_id,
                id end_id
            from course_info
        """
       relations = self.mysql_reader.read(sql)
       self.neo4j_writer.write_relations('HAVE', 'CourseInfo', 'Price', relations)

    def sync_user_info(self):
        sql="""
                select
                    id,
                    real_name name,
                    birthday,
                     CASE gender
                        WHEN 'F' THEN '男'
                        WHEN 'M' THEN '女'
                        ELSE 'unknown'
                    END as gender_translated
                from user_info
            """
        properties = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch AS item
                MERGE (n:StudentInfo {id:item.id,name:item.name,birthday:item.birthday,gender:item.gender_translated})
        """
        self.driver.execute_query(cypher, {"batch": properties})

    def sync_user_info_to_course_info(self):
        sql="""
            select
                user_id start_id,
                course_id end_id,
                create_time add_info
            from favor_info
        """
        relations = self.mysql_reader.read(sql)
        # self.neo4j_writer.write_stu_relation('FAVOR', 'StudentInfo', 'CourseInfo', relations)
        cypher = """
                   UNWIND $batch AS item
                       MATCH (start:StudentInfo {id:item.start_id}),(end:CourseInfo {id:item.end_id})
                       MERGE (start)-[:FAVOR {favor:item.add_info}]->(end)
                   """
        self.driver.execute_query(cypher, {"batch": relations})


    def sync_user_info_to_test_question_info(self):
        sql="""
            select
                user_id start_id,
                question_id end_id,
                CASE is_correct
                    WHEN 0 THEN '错误'
                    WHEN 1 THEN '正确'
                END as add_info 
            from test_exam_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_stu_relation('ANSWER', 'StudentInfo', 'TestQuestionInfo', relations)

        cypher = """
                           UNWIND $batch AS item
                               MATCH (start:StudentInfo {id:item.start_id}),(end:TestQuestionInfo {id:item.end_id})
                               MERGE (start)-[:ANSWER {answer:item.add_info}]->(end)
                           """
        self.driver.execute_query(cypher, {"batch": relations})


    def sync_user_info_to_video_info(self):
        sql = """
                    select
                        a.user_id start_id, 
                        v.id end_id,
                        a.position_sec add_info,
                        v.create_time add_info2
                    from user_chapter_progress a
                        JOIN video_info v
                            ON a.chapter_id=v.chapter_id AND a.course_id=v.course_id
                """
        relations = self.mysql_reader.read(sql)
        cypher = """
               UNWIND $batch AS item
                   MATCH (start:StudentInfo {id:item.start_id}),(end:VideoInfo {id:item.end_id})
                   MERGE (start)-[:WATCH {watch:item.add_info,last_time:item.add_info2}]->(end)
               """
        self.driver.execute_query(cypher, {"batch": relations})

if __name__ == '__main__':
    synchronizer = TableSynchronizer()
    # synchronizer.sync_base_category_info()
    # synchronizer.sync_base_subject_info()
    # synchronizer.sync_course_info()
    # synchronizer.sync_teacher_name()
    # synchronizer.sync_price()
    # synchronizer.sync_chapter_info()
    # synchronizer.sync_video_info()
    # synchronizer.sync_test_paper()
    # synchronizer.sync_test_question_info()
    # synchronizer.sync_course_info_to_base_subject_info()
    # synchronizer.sync_base_subject_info_to_base_category_info()
    # synchronizer.sync_vido_info_to_chapter_info()
    # synchronizer.sync_chapter_info_to_course_info()
    # synchronizer.sync_test_question_info_to_test_paper()
    # synchronizer.sync_test_paper_to_course_info()
    # synchronizer.sync_course_info_to_teacher_info()
    # synchronizer.sync_course_info_to_price()
    # synchronizer.sync_user_info()
    # synchronizer.sync_user_info_to_course_info()
    # synchronizer.sync_user_info_to_test_question_info()
    synchronizer.sync_user_info_to_video_info()