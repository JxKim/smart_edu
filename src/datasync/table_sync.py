from neo4j import GraphDatabase

from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
        self.reader = MysqlReader()
        self.writer = Neo4jWriter()

    def sync_category(self):
        sql = """
        select id category_id,category_name name
        from base_category_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("category", properties)

    def sync_subject(self):
        sql = """
        select id subject_id,subject_name name
        from base_subject_info"""
        properties = self.reader.read(sql)
        print(properties)

        self.writer.write_nodes("subject", properties)

    def sync_course(self):
        sql = """
                select id course_id,course_name name
                from course_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("course", properties)

    def sync_teacher(self):
        sql = """select distinct teacher teacher_name
                from course_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("teacher", properties)
    def sync_price(self):
        sql = """select  origin_price price
                from course_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("price", properties)

    def sync_chapter(self):
        sql = """select  id chapter_id,chapter_name name
                from chapter_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("chapter", properties)

    def sync_video(self):
        sql = """select  id video_id,video_name name
                from video_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("video", properties)

    def sync_paper(self):
        sql = """select  id paper_id,paper_title name
                from test_paper"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("paper", properties)

    def sync_question(self):
        sql = """select  id question_id,question_txt name
                from test_question_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("question", properties)

    def sync_course_to_subject(self):
        sql = """select id start_id,subject_id end_id
        from course_info
        """
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("BELONG", "course", "subject", course_to_subject, start_key="course_id", end_key="subject_id")

    def sync_subject_to_category(self):
        sql = """select id start_id,category_id end_id
                from base_subject_info
                """
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("BELONG", "subject", "category", course_to_subject, start_key="subject_id", end_key="category_id")

    def sync_video_to_chapter(self):
        sql = """select id start_id,chapter_id end_id
                 from video_info
                        """
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("BELONG", "video", "chapter", course_to_subject, start_key="video_id",end_key="chapter_id")

    def sync_chapter_to_course(self):
        sql = """select id start_id,course_id end_id
                 from video_info"""
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("BELONG", "chapter", "course", course_to_subject, start_key="chapter_id",
                                    end_key="course_id")

    def sync_paper_to_course(self):
        sql = """select id start_id,course_id end_id
                 from test_paper"""
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("BELONG", "paper", "course", course_to_subject, start_key="paper_id",
                                    end_key="course_id")

    def sync_question_to_paper(self):
        sql = """select question_id start_id,paper_id end_id
                from test_paper_question"""
        question_to_paper = self.reader.read(sql)
        self.writer.write_relations("BELONG", "question", "paper", question_to_paper, start_key="question_id",
                                    end_key="paper_id")

    def sync_course_to_teacher(self):
        sql = """select id start_id,teacher end_id
                 from course_info"""
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("HAVE", "course", "teacher", course_to_subject, start_key="course_id",
                                    end_key="teacher_name")

    def sync_course_to_price(self):
        sql = """select id start_id,origin_price end_id
                 from course_info"""
        course_to_subject = self.reader.read(sql)
        self.writer.write_relations("HAVE", "course", "price", course_to_subject, start_key="course_id",
                                    end_key="price")

    def sync_student(self):
        sql = """select id uid,
                birthday as "生日",
                gender as "性别"
                from user_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("student", properties)

    def sync_student_to_course(self):
        sql = """SELECT user_id, course_id, create_time
                 FROM favor_info"""
        student_to_course = self.reader.read(sql)

        cypher = """
        UNWIND $batch AS item
        MATCH (s:student {uid: item.user_id})
        MATCH (c:course {course_id: item.course_id})
        MERGE (s)-[r:FAVOR]->(c)
        SET r.time = datetime(item.create_time)
        """
        self.driver.execute_query(cypher, batch=student_to_course)

    def sync_student_to_question(self):
        sql = """select user_id, question_id, is_correct
                from test_exam_question"""
        student_to_question = self.reader.read(sql)
        cypher = """
        UNWIND $batch AS item
        MATCH (s:student {uid: item.user_id})
        MATCH (q:question {question_id: item.question_id})
        MERGE (s)-[r:ANSWER]->(q)
        SET r.is_correct = item.create_time
        """
        self.driver.execute_query(cypher, batch=student_to_question)

    def sync_student_to_chapter(self):
        sql = """select user_id, chapter_id, create_time
        from user_chapter_progress"""
        student_to_chapter = self.reader.read(sql)
        cypher = """
        UNWIND $batch AS item
        MATCH (s:student {uid: item.user_id})
        MATCH (c:chapter {chapter_id: item.chapter_id})
        MERGE (s)-[r:WATCH]->(c)
        SET r.last_watch_time = datetime(item.create_time)
        """
        self.driver.execute_query(cypher, batch=student_to_chapter)



# 学生行为记录
#     来源
#         数据库
#     节点
#         (:学生 {uid,生日,性别})
#     关系
#         学生-[:FAVOR {时间}]->课程
#         学生-[:ANSWER {是否正确}]->试题
#         学生-[:WATCH {进度,最后观看时间}]->章节视频






if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    # synchronizer.sync_category()
    # synchronizer.sync_subject()
    # synchronizer.sync_course()
    # synchronizer.sync_teacher()
    # synchronizer.sync_price()
    # synchronizer.sync_chapter()
    # synchronizer.sync_video()
    # synchronizer.sync_paper()
    # synchronizer.sync_question()
    # synchronizer.sync_course_to_subject()
    # synchronizer.sync_subject_to_category()
    # synchronizer.sync_video_to_chapter()
    # synchronizer.sync_chapter_to_course()
    # synchronizer.sync_paper_to_course()
    # synchronizer.sync_question_to_paper()
    # synchronizer.sync_course_to_teacher()
    # synchronizer.sync_course_to_price()

    # synchronizer.sync_student()
    # synchronizer.sync_student_to_course()
    # synchronizer.sync_student_to_question()
    synchronizer.sync_student_to_chapter()





