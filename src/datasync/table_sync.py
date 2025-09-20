from datasync.utils import MysqlReader, Neo4jWriter



class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def sync_category(self):
        sql = """
                SELECT id,
                category_name name 
                FROM base_category_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Category', properties)

    def sync_subject(self):
        sql = """
                SELECT id,
                subject_name name
                FROM base_subject_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Subject', properties)

    def sync_course(self):
        sql = """
                SELECT id,
                course_name name
                FROM course_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Course', properties)

    def sync_teacher(self):
        sql = """
                SELECT teacher name
                FROM course_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_name('Teacher', properties)

    def sync_price(self):
        sql = """
                SELECT 
                id id,
                CAST(origin_price AS CHAR) AS name
                FROM course_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_price('Price', properties)

    def sync_chapter(self):
        sql = """
                SELECT id,
                chapter_name name
                FROM chapter_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Chapter', properties)

    def sync_video(self):
        sql = """
                SELECT id,
                video_name name
                FROM video_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Video', properties)

    def sync_paper(self):
        sql = """
                SELECT id,
                paper_title name
                FROM test_paper
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Paper', properties)

    def sync_question(self):
        sql = """
                SELECT id,
                question_txt name
                FROM test_question_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Question', properties)

    def sync_course_to_subject(self):
        sql = """
                 select id start_id,
                        subject_id     end_id
                 from course_info
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Course", "Subject", relations)

    def sync_subject_to_category(self):
        sql = """
                 select id start_id,
                        category_id     end_id
                 from base_subject_info
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Subject", "Category", relations)

    def sync_video_to_chapter(self):
        sql = """
                 select id start_id,
                        chapter_id     end_id
                 from video_info
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Video", "Chapter", relations)

    def sync_chapter_to_course(self):
        sql = """
                 select id start_id,
                        course_id     end_id
                 from chapter_info
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Chapter", "Course", relations)

    def sync_question_to_paper(self):
        sql = """
                 select question_id start_id,
                        paper_id     end_id
                 from test_paper_question
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Question", "Paper", relations)

    def sync_paper_to_course(self):
        sql = """
                 select id start_id,
                        course_id     end_id
                 from test_paper
                 """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Paper", "Course", relations)


    def sync_course_to_teacher(self):
        sql = """
                    select id start_id,
                           teacher     end_id
                    from course_info
                    """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_course_to_teacher_relations("Have", "Course", "Teacher", relations)

    def sync_course_to_price(self):
        sql = """
                    select id start_id,
                           cast(origin_price as char)     end_id
                    from course_info
                    """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_course_to_price_relations("Have", "Course", "Price", relations)

    def sync_user(self):
        sql = """
                SELECT id,birthday,
                COALESCE(gender,'UNKNOWN') gender
                FROM user_info
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_user('User', properties)

    def sync_user_to_course(self):
        sql = """
                    select user_id start_id,
                           course_id    end_id,
                           create_time favor_time
                    from favor_info
                    """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_stu_relations("Favor", "User", "Course", relations)



if __name__ == '__main__':
    synchronizer = TableSynchronizer()
    synchronizer.sync_category()
    synchronizer.sync_subject()
    synchronizer.sync_course()
    synchronizer.sync_teacher()
    synchronizer.sync_price()
    synchronizer.sync_chapter()
    synchronizer.sync_video()
    synchronizer.sync_paper()
    synchronizer.sync_question()
    synchronizer.sync_course_to_subject()
    synchronizer.sync_subject_to_category()
    synchronizer.sync_video_to_chapter()
    synchronizer.sync_chapter_to_course()
    synchronizer.sync_question_to_paper()
    synchronizer.sync_paper_to_course()
    synchronizer.sync_course_to_teacher()
    synchronizer.sync_course_to_price()
    synchronizer.sync_user()
    synchronizer.sync_user_to_course()


