from datasync.utils import MysqlReader, Neo4jWriter

class TableSync:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def categoryInfo_sync(self):
        sql = '''
                select id,
                       category_name name
                from base_category_info
            '''
        c2 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('CategoryInfo', c2)

    def subjectInfo_sync(self):
        sql = """
              select id,
                     subject_name name
              from base_subject_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('SubjectInfo', c1)

    def courseInfo_sync(self):
        sql = """
                  select id,
                         course_name name
                  from course_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('CourseInfo', c1)

    def subject_to_category_sync(self):
        sql = '''
            select id s_id,
                category_id e_id
            from base_subject_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('SubjectInfo', 'Belong', 'CategoryInfo', c2_to_c1)

    def course_to_subject_sync(self):
        sql = '''
            select id s_id,
                subject_id e_id
            from course_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('CourseInfo', 'Belong', 'SubjectInfo', c2_to_c1)

    def chapterInfo_sync(self):
        sql = """
                  select id,
                         chapter_name name
                  from chapter_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('ChapterInfo', c1)

    def chapter_to_course_sync(self):
        sql = '''
            select id s_id,
                course_id e_id
            from chapter_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('ChapterInfo', 'Belong', 'CourseInfo', c2_to_c1)

    def videoInfo_sync(self):
        sql = """
                  select id,
                         video_name name
                  from video_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('VideoInfo', c1)

    def video_to_chapter_sync(self):
        sql = '''
            select id s_id,
                chapter_id e_id
            from video_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('VideoInfo', 'Belong', 'ChapterInfo', c2_to_c1)


    def paperInfo_sync(self):
        sql = """
                  select id,
                         paper_title name
                  from test_paper
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('PaperInfo', c1)

    def paper_to_course_sync(self):
        sql = '''
            select id s_id,
                course_id e_id
            from test_paper
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('PaperInfo', 'Belong', 'CourseInfo', c2_to_c1)

    def questionInfo_sync(self):
        sql = """
                  select id,
                         question_txt name
                  from test_question_info
                  """
        c1 = self.mysql_reader.read(sql)
        # c1['question_txt'] = c1['question_txt'].replace('</p>','').replace('<p>','')
        self.neo4j_writer.build_nodes('QuestionInfo', c1)

    def question_to_paper_sync(self):
        sql = '''
            select paper_id e_id,
                question_id s_id
            from test_paper_question
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('QuestionInfo', 'Belong', 'PaperInfo', c2_to_c1)

    def studentInfo_sync(self):
        sql = """
                  select id,
                         real_name name
                  from user_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('StudentInfo', c1)

    def student_to_course_sync(self):
        sql = '''
            select course_id e_id,
                user_id s_id,
                create_time other
            from favor_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('StudentInfo', 'Favor', 'CourseInfo', c2_to_c1, 'time')

    def student_to_question_sync(self):
        sql = '''
            select question_id e_id,
                user_id s_id,
                IF(is_correct = 1, '正确', '错误') AS other
            from test_exam_question
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('StudentInfo', 'Answer', 'QuestionInfo', c2_to_c1, 'correct')

    def knowledgePointInfo_sync(self):
        sql = """
                  select id,
                         point_txt name
                  from knowledge_point
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('KnowledgePoint', c1)

    def knowledgePoint_to_question_sync(self):
        sql = '''
            select question_id s_id,
                point_id e_id
            from test_point_question
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('QuestionInfo', 'Test', 'KnowledgePoint', c2_to_c1)

    def orderInfo_sync(self):
        sql = """
                  select id,
                         trade_body name
                  from order_info
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('OrderInfo', c1)

    def order_to_user_sync(self):
        sql = '''
            select id e_id,
                user_id s_id
            from order_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('StudentInfo', 'Have', 'OrderInfo', c2_to_c1)

    def orderDetail_sync(self):
        sql = """
                  select id,
                         course_name name
                  from order_detail
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('OrderDetail', c1)

    def orderDetail_to_order_sync(self):
        sql = '''
            select order_id s_id,
                id e_id
            from order_detail
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('OrderInfo', 'Have', 'OrderDetail', c2_to_c1)

    def questionOption_sync(self):
        sql = """
                  select id,
                        option_txt name
                  from test_question_option
                  where option_txt is not Null
                  """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('QuestionOption', c1)

    def questionOption_to_question_sync(self):
        sql = '''
            select question_id s_id,
                id e_id
            from test_question_option
            where option_txt is not Null
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('QuestionInfo', 'Have', 'QuestionOption', c2_to_c1)

    def teacher_sync(self):
        sql = """
              select teacher id,
                    teacher name
              from course_info
              """
        c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_nodes('Teacher', c1)

    def tacher_to_course_sync(self):
        sql = '''
            select teacher s_id,
                id e_id
            from course_info
        '''
        c2_to_c1 = self.mysql_reader.read(sql)
        self.neo4j_writer.build_relations('Teacher', 'Teach', 'CourseInfo', c2_to_c1)


if __name__ == '__main__':
    table_sync = TableSync()

    # 构建课程/学科/分类
    # table_sync.categoryInfo_sync()
    # table_sync.subjectInfo_sync()
    # table_sync.courseInfo_sync()
    # table_sync.subject_to_category_sync()
    # table_sync.course_to_subject_sync()
    #
    # # 构建课程/章节/视频
    # table_sync.chapterInfo_sync()
    # table_sync.videoInfo_sync()
    # table_sync.chapter_to_course_sync()
    # table_sync.video_to_chapter_sync()
    #
    # # 构建课程/试卷/试题
    # table_sync.questionInfo_sync()
    # table_sync.paperInfo_sync()
    # table_sync.question_to_paper_sync()
    # table_sync.paper_to_course_sync()
    #
    # # 构建学生/课程/试题
    # table_sync.studentInfo_sync()
    # table_sync.student_to_course_sync()
    table_sync.student_to_question_sync()
    #
    # # # 问题/知识点
    # table_sync.knowledgePointInfo_sync()
    # table_sync.knowledgePoint_to_question_sync()
    #
    # # 用户/订单/订单详情
    # table_sync.orderInfo_sync()
    # table_sync.orderDetail_sync()
    # table_sync.orderDetail_to_order_sync()
    # table_sync.order_to_user_sync()
    #
    # # 题目/选项
    # table_sync.questionOption_sync()
    # table_sync.questionOption_to_question_sync()

    # 老师/课程
    table_sync.teacher_sync()
    table_sync.tacher_to_course_sync()