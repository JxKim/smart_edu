from datasync.utils import MysqlReader, Neo4jWriter

class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def sync_category(self):
        sql = 'select id, category_name as name from base_category_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("category", properties)

    def sync_subject(self):
        sql = 'select id, subject_name as name from base_subject_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("subject", properties)

    def sync_course(self):
        sql = 'select id, course_name as name from course_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("course", properties)

    def sync_teacher(self):
        sql = 'select distinct teacher as id, teacher as name from course_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("teacher", properties)

    def sync_chapter(self):
        sql = 'select id, chapter_name as name from chapter_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("chapter", properties)

    def sync_video(self):
        sql = 'select id, video_name as name from video_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("video", properties)

    def sync_paper(self):
        sql = 'select id, paper_title as name from test_paper'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("paper", properties)

    def sync_question(self):
        sql = 'select id, question_txt as name from test_question_info'
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("question", properties)

    def sync_subject_to_category(self):
        sql = 'select id as start_id, category_id as end_id from base_subject_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'subject', 'category', relations)

    def sync_course_to_subject(self):
        sql = 'select id as start_id, subject_id as end_id from course_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'course', 'subject', relations)

    def sync_chapter_to_course(self):
        sql = 'select id as start_id, course_id as end_id from chapter_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'chapter', 'course', relations)

    def sync_video_to_chapter(self):
        sql = 'select id as start_id, chapter_id as end_id from video_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'video', 'chapter', relations)

    def sync_paper_to_course(self):
        sql = 'select id as start_id, course_id as end_id from test_paper'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'paper', 'course', relations)

    def sync_question_to_course(self):
        sql = 'select id as start_id, course_id from test_question_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'question', 'course', relations)

    def sync_course_to_teacher(self):
        sql = 'select id as start_id, teacher as end_id from course_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('HAVE', 'course', 'teacher', relations)

    def sync_students(self):
        sql = 'select id, real_name as name, gender, birthday from user_info'
        properties = self.mysql_reader.read(sql)
        for property in properties:
            if property['gender'] is None:
                property['gender'] = 'Unknown'
        self.neo4j_writer.write_students("student", properties)

    def sync_favorites(self):
        sql = 'select user_id as start_id, course_id as end_id from favor_info'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('FAVOR', 'student', 'course', relations)

    def sync_student_to_test(self):
        sql = 'select user_id as start_id, paper_id as end_id, CAST(score AS FLOAT) AS score from test_exam'
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_scores('ANSWER', 'student', 'paper', relations)


if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    # 同步节点
    # synchronizer.sync_category()
    # synchronizer.sync_subject()
    # synchronizer.sync_course()
    # synchronizer.sync_teacher()
    # synchronizer.sync_chapter()
    # synchronizer.sync_video()
    # synchronizer.sync_paper()
    # synchronizer.sync_question()
    # synchronizer.sync_students()

    # 同步关系
    # synchronizer.sync_subject_to_category()
    # synchronizer.sync_course_to_subject()
    # synchronizer.sync_chapter_to_course()
    # synchronizer.sync_video_to_chapter()
    # synchronizer.sync_paper_to_course()
    # synchronizer.sync_question_to_course()
    # synchronizer.sync_course_to_teacher()
    synchronizer.sync_favorites()
    synchronizer.sync_student_to_test()