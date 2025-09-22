import datetime
import random

from datasync.read_write_utils import MysqlReader, Neo4jWriter


class TableSync:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 🔥课程数据资料
    # 1.课程分类节点
    def sync_category(self):
        sql = """
        select id,category_name as name from base_category_info

        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('CourseCategory', properties)

    # 2.学科节点
    def sync_subject_id(self):
        sql = """
        select id,subject_name as name from base_subject_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('SubjectId', properties)

    # 3.课程节点
    def sync_course(self):
        sql = """
        select id,course_name as name from course_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Course', properties)

    # 4.教师节点
    def sync_teacher(self):
        sql = '''
        select id, teacher as teacher_name from course_info
        '''
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Teacher', properties)

    # 5.价格节点
    def sync_price(self):
        sql = '''
        select id,course_name, actual_price as price from course_info
        '''
        properties = self.mysql_reader.read(sql)
        for prop in properties:
            prop['price'] = float(prop['price'])
        self.neo4j_writer.write_nodes('Price', properties)

    # 6.章节节点
    def sync_chapter(self):
        sql = '''
        select id,course_id,video_id,chapter_name 
        from chapter_info
        '''
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Chapter', properties)

    # 7.视频节点
    def sync_video(self):
        sql = '''
        select id,video_name 
        from video_info
        '''
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Video', properties)

    # 8.试卷节点
    def sync_paper(self):
        sql = '''
        select id,course_id,paper_title
        from test_paper
        '''
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('Paper', properties)

    # 9.试题节点
    def sync_test_question(self):
        sql = '''
        select id,course_id,question_txt
        from test_question_info
        '''
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes('TestQuestion', properties)



    # 🐦---------------------------关系---------------------------
    # 1.课程 belong 学科
    def sync_course_belong_subject_id(self):
        sql = '''
        select id as start_id,subject_id as end_id
        from course_info
        '''
        relations = self.mysql_reader.read(sql)
        # import pdb;pdb.set_trace()
        self.neo4j_writer.write_relations('BELONG', 'Course', 'SubjectId', relations)
        print('🍉课程 --belong--> 学科')
    # 学科 belong 课程分类
    def sync_subject_id_belong_category(self):
        sql = '''
        select id as start_id,category_id as end_id
        from base_subject_info
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'SubjectId', 'CourseCategory', relations)
        print('🍉学科 --belong--> 课程分类')

    # 2.视频 belong 章节
    def sync_video_belong_chapter(self):
        sql = '''
        select id as start_id,chapter_id as end_id
        from video_info
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'Video', 'Chapter', relations)
        print('🍑视频 --belong--> 章节')

    # 章节 belong 课程
    def sync_chapter_belong_course(self):
        sql = '''
        select id as start_id,course_id as end_id
        from chapter_info
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'Chapter', 'Course', relations)
        print('🍑章节 --belong--> 课程')

    # 3.试题属于试卷
    def sync_test_question_belong_paper(self):
        sql = '''
        select question_id as start_id, paper_id as end_id
        from  test_paper_question
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'TestQuestion', 'Paper', relations)
        print('🥝试题 --belong--> 试卷')

    # 试卷属于课程
    def sync_paper_belong_course(self):
        sql = '''
        select id as start_id,course_id as end_id
        from test_paper
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('BELONG', 'Paper', 'Course', relations)
        print('🥝试卷 --belong--> 课程')

    # 4.课程有教师
    def sync_course_have_teacher(self):
        sql = '''
        select id as start_id,id as end_id
        from course_info
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('HAVE', 'Course', 'Teacher', relations)
        print('🍓课程 --have--> 教师')

    # 5.课程有价格
    def sync_course_have_price(self):
        sql = '''
        select id as start_id, id as end_id
        from course_info 
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations('HAVE', 'Course', 'Price', relations)
        print('🍓课程 --have--> 价格')


#--------------------------------------------------------------------------------------------
    # 🔥学生行为记录
    #1.学生节点
    def sync_student(self):
        sql = '''
        select id ,real_name,birthday ,gender
        from user_info
        '''
        properties = self.mysql_reader.read(sql)
        for prop in properties:
            gender = prop.get('gender')
            # 处理性别
            if gender == 'M':
                prop['gender'] = '男'
            elif gender == 'F':
                prop['gender'] = '女'
            else:
                prop['gender'] = '未知'
        self.neo4j_writer.write_nodes('Student', properties)

    # 🐦---------------------------关系------------------------------
    # 1.学生 -- FAVOR{时间} --> 课程
    def sync_student_favor_course(self):
        sql = '''select user_id as start_id, course_id as end_id, create_time as favor_time
        from user_chapter_progress
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations_with_propert('FAVOR', 'Student', 'Course', relations)
        print('🍉学生 --favor--> 课程')
    # 2.学生 -- ANSWER{是否正确} --> 试题
    def sync_student_answer_test_question(self):
        sql = '''select user_id as start_id , question_id as end_id, is_correct
        from test_exam_question
        '''
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations_with_propert('ANSWER', 'Student', 'TestQuestion', relations)
    # 3.学生 -- WATCH{进度,最后观看时间}] --> 章节视频
    def sync_student_watch_chapter_video(self):
        sql = '''select user_id as start_id, chapter_id as end_id, position_sec as 进度, create_time
        from user_chapter_progress
        '''
        relations = self.mysql_reader.read(sql)
        #处理每条关系数据的‘最后观看时间’
        for relation in relations:
            create_time = relation.get('create_time')
            if create_time:
                random_seconds = random.randint(0, 7200) # 随机数 0-7200秒(2h)
                update_datetime = create_time + datetime.timedelta(seconds=random_seconds)
                relation['最后观看时间'] = update_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                relation['最后观看时间'] = '未观看'
        self.neo4j_writer.write_relations_with_propert('WATCH', 'Student', 'Chapter', relations)

#--------------------------------------------------------------------------------------------
    # 🔥学生行为记录


if __name__ == '__main__':
    table_sync = TableSync()
    #❗创建节点
    #🍅课程数据资料--节点
    table_sync.sync_category()
    table_sync.sync_subject_id()
    table_sync.sync_course()
    table_sync.sync_teacher()
    table_sync.sync_price()
    table_sync.sync_chapter()
    table_sync.sync_video()
    table_sync.sync_paper()
    table_sync.sync_test_question()

    # 🍅课程数据资料--关系
    table_sync.sync_course_belong_subject_id()
    print('🍅课程 --belong--> 科目')
    table_sync.sync_subject_id_belong_category()
    print('🍅课程 --belong--> 课程分类')
    table_sync.sync_video_belong_chapter()
    print('🍑视频 --belong--> 章节')
    table_sync.sync_chapter_belong_course()
    print('🍑章节 --belong--> 课程')
    table_sync.sync_test_question_belong_paper()
    print('🥝试题 --belong--> 试卷')
    table_sync.sync_paper_belong_course()
    print('🥝试卷 --belong--> 课程')
    table_sync.sync_course_have_teacher()
    print('🍓课程 --have--> 教师')
    table_sync.sync_course_have_price()
    print('🍓课程 --have--> 价格')
    #------------------------------------------------------------------


    #🍋学生行为记录--节点
    table_sync.sync_student()

    # 🍋学生行为记录--关系
    table_sync.sync_student_favor_course()
    print('🍉学生 --favor--> 课程')
    table_sync.sync_student_answer_test_question()
    print('🍉学生 --answer--> 试题')
    table_sync.sync_student_watch_chapter_video()
    print('🍉学生 --watch--> 章节视频')


