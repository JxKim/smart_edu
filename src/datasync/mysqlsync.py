from datasync.utils import MysqlUtil, Neo4jUtil


class MysqlSync:
    def __init__(self):
        self.mysql_util = MysqlUtil()
        self.neo4j_util = Neo4jUtil()

    def create_category(self):
        sql = """
            select id,
                category_name name
            from base_category_info
            """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Category', properties)
    def create_subject(self):
        sql = """
            select id,
                subject_name name
            from base_subject_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Subject', properties)
    def create_chapter(self):
        sql = """
            select id,
                chapter_name name,
                is_free is_free
            from chapter_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Chapter', properties)
    def create_knowledge_point(self):
        sql = """
            select id,
                point_txt txt
            from knowledge_point
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('KnowledgePoint', properties)
    def create_course(self):
        sql = """
            select      id,
                course_name name,
                chapter_num ,
                origin_price price
            from course_info
        """
        properties = self.mysql_util.read(sql)
        for property in properties:
            property['price'] = float(property['price'])
        self.neo4j_util.writer_nodes('Course', properties)
    def create_exam_paper(self):
        sql = """
            select id,
                paper_title title
            from test_paper
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('ExamPaper', properties)
    def create_question(self):
        sql = """
            select id,
                question_txt question
            from test_question_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Question', properties)
    def create_question_answer(self):
        sql = """
            select id,
                option_txt answer
            from test_question_option 
            where is_correct = 1
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Answer', properties)
    def create_user(self):
        sql = """
            select id,
                login_name
            from user_info 
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('User', properties)
    def create_video(self):
        sql = """
            select id,
                video_name video,
                during_sec time
            from video_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Video', properties)
    def create_teacher(self):
        sql = """
            select id,
                teacher_name name
            from teacher_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Teacher', properties)
    def create_comment(self):
        sql = """
            select id,
                comment_txt comment
            from comment_info
        """
        properties = self.mysql_util.read(sql)
        self.neo4j_util.writer_nodes('Comment', properties)


    def create_relations_subject2category(self):
        sql = """
            select id start_id,
                 category_id end_id
            from base_subject_info       
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Belong', 'Subject', 'Category', relations)
    def create_relations_chapter2course(self):
        sql = """
            select id start_id,
                course_id end_id
            from chapter_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Belong', 'Chapter', 'Course', relations)
    def create_relations_chapter2video(self):
        sql = """
            select id start_id, 
                video_id end_id
            from chapter_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Explained', 'Chapter', 'Video', relations)


    def create_relations_user2comment(self):
        sql = """
            select user_id start_id,
                        id end_id
            from comment_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Comment', 'User', 'Comment', relations)
    def create_relations_teacher2course(self):
        sql = """
            select c.id end_id,
                t.id start_id
            from course_info c 
                join teacher_info t on c.teacher = t.teacher_name
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Teach', 'Teacher', 'Course', relations)
    def create_relations_course2subject(self):
        sql = """
            select id start_id,
                subject_id end_id
            from course_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Belong', 'Course', 'Subject', relations)
    def create_relations_students_favor_course(self):
        sql = """
            select user_id start_id,
                course_id end_id,
            from favor_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations_attr('Favor', 'User', 'Course', relations)
    def create_relations_users_review_course(self):
        sql = """
            select user_id start_id,
                review_stars review,
                course_id end_id
            from review_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations_attr('Review', 'User', 'Course', relations)


    def create_relations_users2exam_paper(self):
        sql = """
            select paper_id end_id,
                user_id start_id,
                score
            from test_exam
        """
        relations = self.mysql_util.read(sql)
        for relation in relations:
            relation['score'] = float(relation['score'])
        self.neo4j_util.writer_relations_attr('Test', 'User', 'ExamPaper', relations)
    def create_relations_paper2course(self):
        sql = """
            select id start_id,
                course_id end_id
            from test_paper
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Belong', 'ExamPaper', 'Course', relations)
    def create_relations_questions2paper(self):
        sql = """
            select question_id start_id,
                    paper_id end_id,
                    score
            from test_exam_question
        """
        relations = self.mysql_util.read(sql)
        for relation in relations:
            relation['score'] = float(relation['score'])
        self.neo4j_util.writer_relations_attr('Belong', 'Question', 'ExamPaper', relations)
    def create_relations_questions2knowledge(self):
        sql = """
            select question_id start_id,
                    point_id end_id
            from test_point_question
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations('Test', 'Question', 'KnowledgePoint', relations)
    def create_relations_questions2course(self):
        sql = """
            select id start_id,
                course_id end_id
            from test_question_info
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations_attr('Belong', 'Question', 'Course', relations)

    def create_relations_users2video(self):
        sql = """
            select u.user_id start_id,
                        v.id end_id,
                      position_sec  record
            from user_chapter_progress u 
            join video_info v 
            on u.chapter_id = v.chapter_id
        """
        relations = self.mysql_util.read(sql)
        self.neo4j_util.writer_relations_attr('Watch', 'User', 'Video', relations)

if __name__ == "__main__":
    create = MysqlSync()


    # create.create_category()
    # create.create_subject()
    # create.create_chapter()
    # create.create_knowledge_point()
    # create.create_course()
    # create.create_exam_paper()
    # create.create_question()
    # create.create_question_answer()
    # create.create_user()
    # create.create_video()
    # create.create_comment()
    # create.create_teacher()

    # create.create_relations_subject2category()
    # create.create_relations_chapter2course()
    # create.create_relations_chapter2video()
    #
    # create.create_relations_user2comment()
    #
    # create.create_relations_course2subject()
    #
    # create.create_relations_users_review_course()
    #
    # create.create_relations_users2exam_paper()
    # create.create_relations_paper2course()
    # create.create_relations_questions2paper()
    #
    # create.create_relations_questions2course()
    #
    # create.create_relations_users2video()


    # create.create_relations_teacher2course()
    # create.create_relations_students_favor_course()
    # create.create_relations_questions2knowledge()