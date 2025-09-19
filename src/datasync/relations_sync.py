from datasync.utils import Neo4jWriter, MysqlReader


class RelationsSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 课程-BELONG->学科
    def sync_course_belong_subject(self):
        sql = """
            select 
                id start_id,
                subject_id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('BELONG', 'Course', 'Subject', relations)
        print('课程-BELONG->学科同步完成')

    # 学科-BELONG->分类
    def sync_subject_belong_category(self):
        sql = """
            select 
                id start_id,
                category_id end_id
            from base_subject_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('BELONG', 'Subject', 'Category', relations)
        print('学科-BELONG->分类同步完成')

    # 视频-BELONG->章节
    def sync_video_belong_chapter(self):
        sql = """
            select 
                id start_id,
                course_id end_id
            from video_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('BELONG', 'Video', 'Chapter', relations)
        print('视频-BELONG->章节同步完成')

    # 章节-BELONG->课程
    def sync_chapter_belong_course(self):
        sql = """
            select 
                id start_id,
                course_id end_id
            from chapter_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('BELONG', 'Chapter', 'Course', relations)
        print('章节-BELONG->课程同步完成')

    # 课程-HAVE->教师
    def sync_course_have_teacher(self):
        sql = """
            select 
                id start_id,
                id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('HAVE', 'Course', 'Teacher', relations)
        print('课程-HAVE->教师同步完成')

    # 课程-HAVE->价格
    def sync_course_have_price(self):
        sql = """
            select 
                id start_id,
                id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('HAVE', 'Course', 'Price', relations)
        print('课程-HAVE->价格同步完成')




    # 学生-FAVOR->课程
    def sync_student_favor_course(self):
        sql = """
            select 
                user_id start_id,
                course_id end_id,
                DATE_FORMAT(create_time, '%Y-%m-%d') property
            from favor_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations_with_property('FAVOR', 'Student', 'Course', '收藏时间', relations)
        print('学生-FAVOR->课程同步完成')

    # 学生-ANSWER->试题
    def sync_student_answer_question(self):
        sql = """
            select 
                user_id start_id,
                question_id end_id,
                is_correct property
            from test_exam_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations_with_property('ANSWER', 'Student', 'Question', '答题结果', relations)
        print('学生-ANSWER->试题同步完成')

    # 学生-WATCH->章节视频
    def sync_student_watch_video(self):
        pass





    # 课程-HAVE-知识点
    def sync_course_have_knowledge(self):
        sql = """
            select 
                course_id start_id,
                t.point_id end_id
            from test_paper paser 
            join test_paper_question tq on paser.id = tq.paper_id
            join test_point_question t on tq.question_id = t.question_id
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('HAVE', 'Course', 'Knowledge', relations)
        print('课程-HAVE-知识点同步完成')

    # 章节-HAVE-知识点
    def sync_chapter_have_knowledge(self):
        sql = """
            select 
                chapter.id start_id,
                t.point_id end_id
            from chapter_info chapter 
            join course_info course on chapter.course_id = course.id
            join test_paper paser on course.id = paser.course_id
            join test_paper_question tq on paser.id = tq.paper_id
            join test_point_question t on tq.question_id = t.question_id
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('HAVE', 'Chapter', 'Knowledge', relations)
        print('章节-HAVE-知识点同步完成')

    # 试题-HAVE-知识点
    def sync_question_have_knowledge(self):
        sql = """
            select 
                paper.id start_id,
                t.point_id end_id
            from test_paper paper 
            join test_paper_question tq on paper.id = tq.paper_id
            join test_point_question t on tq.question_id = t.question_id
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_relations('HAVE', 'Question', 'Knowledge', relations)
        print('试题-HAVE-知识点同步完成')


    # 先修关系：知识点-NEED->知识点
    # 包含关系：知识点-BELONG->知识点
    # 相关关系：知识点-RELATED->知识点


if __name__ == '__main__':
    relations_synchronizer = RelationsSynchronizer()
    # relations_synchronizer.sync_course_belong_subject()
    # relations_synchronizer.sync_subject_belong_category()
    # relations_synchronizer.sync_video_belong_chapter()
    # relations_synchronizer.sync_chapter_belong_course()
    # relations_synchronizer.sync_course_have_teacher()
    # relations_synchronizer.sync_course_have_price()

    # relations_synchronizer.sync_course_have_knowledge()
    # relations_synchronizer.sync_chapter_have_knowledge()
    # relations_synchronizer.sync_question_have_knowledge()
    # relations_synchronizer.sync_student_favor_course()
    relations_synchronizer.sync_student_answer_question()

