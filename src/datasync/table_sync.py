from datasync.utils import Neo4jWriter, MysqlReader


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 同步课程数据
    def sync_category(self):
        sql = """
            select id, 
                   category_name name
            from base_category_info
        """
        category = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("分类", category)

    def sync_subject(self):
        sql = """
            select id, 
                   subject_name name
            from base_subject_info
        """
        subject = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("学科", subject)

    def sync_course(self):
        sql = """
            select id, 
                   course_name name
            from course_info
        """
        course = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("课程", course)

    def sync_teacher(self):
        sql = """
            select teacher name
            from course_info
        """
        teacher = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes_name("教师", teacher)

    def sync_price(self):
        sql = """
            select distinct cast(origin_price as float) price
            from course_info
        """
        price = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes_price("价格", price)

    def sync_chapter(self):
        sql = """
            select id, 
                   chapter_name name
            from chapter_info
        """
        chapter = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("章节", chapter)

    def sync_video(self):
        sql = """
            select id, 
                   video_name name
            from video_info
        """
        video = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("视频", video)

    def sync_paper(self):
        sql = """
            select id, 
                   paper_title name
            from test_paper
        """
        paper = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("试卷", paper)

    def sync_question(self):
        sql = """
            select id, 
                   question_txt name
            from test_question_info
        """
        question = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("试题", question)

    def sync_course_to_subject(self):
        sql = """
            select id start_id,
                   subject_id end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "课程", "学科", relations)

    def sync_subject_to_category(self):
        sql = """
            select id start_id,
                   category_id end_id
            from base_subject_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "学科", "分类", relations)

    def sync_video_to_chapter(self):
        sql = """
            select video_id start_id,
                   id end_id
            from chapter_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "视频", "章节", relations)

    def sync_chapter_to_course(self):
        sql = """
            select id start_id,
                   course_id end_id
            from chapter_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "章节", "课程", relations)

    def sync_question_to_paper(self):
        sql = """
            select question_id start_id,
                   paper_id end_id
            from test_paper_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "试题", "试卷", relations)

    def sync_paper_to_course(self):
        sql = """
            select id start_id,
                   course_id end_id
            from test_paper
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "试卷", "课程", relations)

    def sync_course_to_teacher(self):
        sql = """
            select id start_id,
                   teacher end_name
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations_name("Have", "课程", "教师", relations)

    def sync_course_to_price(self):
        sql = """
            select id start_id,
                   cast(origin_price as float) end_price
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations_price("Have", "课程", "价格", relations)

    # 同步学生行为数据
    def sync_student(self):
        sql = """
            select id uid, 
                   birthday,
                   ifnull(gender, 'X') as gender
            from user_info
        """
        student = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes_student("学生", student)

    def sync_student_to_course(self):
        sql = """
            select user_id start_id,
                   course_id end_id,
                   create_time time
            from favor_info
        """
        relations = self.mysql_reader.read(sql)
        relations = [
            {'start_id': relation['start_id'], 'end_id': relation['end_id'], 'time': relation['time'].isoformat()} for
            relation in relations]
        self.neo4j_writer.write_relations_favor("Favor", "学生", "课程", relations)

    def sync_student_to_question(self):
        sql = """
            select user_id start_id,
                   question_id end_id,
                   is_correct
            from test_exam_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations_correct("Answer", "学生", "试题", relations)

    def sync_student_to_video(self):
        sql = """
            select
                u.user_id start_id,
                c.video_id end_id,
                u.position_sec,
                ifnull(u.update_time, u.create_time) as update_time
            from user_chapter_progress u
            join chapter_info c on u.chapter_id = c.id
            where u.user_id between 1 and 1000
        """
        relations = self.mysql_reader.read(sql)
        relations = [
            {'start_id': relation['start_id'], 'end_id': relation['end_id'], 'position_sec': relation['position_sec'],
             'update_time': relation['update_time'].isoformat()} for
            relation in relations]
        self.neo4j_writer.write_relations_watch("Watch", "学生", "视频", relations)

    # 同步知识概念体系数据
    # def sync_knowledge(self):
    #     sql = """
    #         select id,
    #                point_txt name
    #         from knowledge_point
    #     """
    #     knowledge = self.mysql_reader.read(sql)
    #     self.neo4j_writer.write_nodes("知识点", knowledge)


if __name__ == "__main__":
    ts = TableSynchronizer()

    # 同步课程数据
    # ts.sync_category()
    # ts.sync_subject()
    # ts.sync_course()
    # ts.sync_teacher()
    # ts.sync_price()
    # ts.sync_chapter()
    # ts.sync_video()
    # ts.sync_paper()
    # ts.sync_question()
    # ts.sync_course_to_subject()
    # ts.sync_subject_to_category()
    # ts.sync_video_to_chapter()
    # ts.sync_chapter_to_course()
    # ts.sync_question_to_paper()
    # ts.sync_paper_to_course()
    # ts.sync_course_to_teacher()
    # ts.sync_course_to_price()

    # 同步学生行为数据
    # ts.sync_student()
    # ts.sync_student_to_course()
    # ts.sync_student_to_question()
    # ts.sync_student_to_video()

    # 同步知识概念体系数据
    # ts.sync_knowledge()
