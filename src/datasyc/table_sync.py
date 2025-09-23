from utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def sync_category(self):
        sql = """
                select id,
                      category_name name
                from base_category_info
            """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("分类", properties)

    def sync_subject(self):
        sql = """
                select id,
                      subject_name name
                from base_subject_info
            """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("学科", properties)

    def sync_chapter(self):
        sql = """
                select id,
                      chapter_name name
                from chapter_info
            """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("章节", properties)


    def sync_video(self):
        sql = """
                select id,
                      video_name name
                from video_info
            """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("视频", properties)

    def sync_test_paper(self):
        sql = """
                select id,
                      paper_title name
                from test_paper
            """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("试卷", properties)

    def sync_test_paper_question(self):
        sql = """
                        select id,
                              question_txt name
                        from test_question_info
                    """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("试题", properties)

    def sync_course(self):
        sql = """
                        select id,
                              course_name name,
                              teacher teacher_name,
                              actual_price price
                        from course_info
                    """
        properties = self.mysql_reader.read(sql)
        for index, item in enumerate(properties):
            properties[index]['price'] = float(item['price'])
        self.neo4j_writer.write_course_nodes("课程", properties)

    def sync_course_to_subject(self):
        sql = """
                        select id           start_id,
                               subject_id end_id
                        from course_info  
                    """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "课程", "学科", relations)
    def sync_subject_to_category(self):
        sql = """
                                select id           start_id,
                                       category_id end_id
                                from base_subject_info  
                            """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "学科", "分类", relations)
    def sync_chapter_to_course(self):
        sql = """
                                        select id           start_id,
                                               course_id end_id
                                        from chapter_info 
                                    """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "章节", "课程", relations)
    def sync_video_to_chapter(self):
        sql = """
                                                select id           start_id,
                                                       chapter_id end_id
                                                from video_info 
                                            """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "视频", "章节", relations)
    def sync_test_paper_to_course(self):
        sql = """
                select id           start_id,
                course_id end_id
                from test_paper 
            """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "试卷", "课程", relations)

    def sync_test_paper_question_to_test_paper(self):
        sql = """
                 select question_id start_id,
                 paper_id end_id
                 from test_paper_question
                        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "试题", "试卷", relations)

    def sync_student(self):
        sql = """
                select id,
                      birthday birth,
                      gender 
                from user_info
            """
        properties = self.mysql_reader.read(sql)
        for index, item in enumerate(properties):
            if item['gender'] is None:
                properties[index]['gender'] = "UNK"
        self.neo4j_writer.write_student_nodes("学生", properties)

    def sync_student_to_course(self):
        sql = """
                 select user_id start_id,
                        course_id end_id,
                        create_time
                 from favor_info
                        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_favor("Favor", "学生", "课程", relations)

    def sync_student_to_question(self):
        sql = """
                 select user_id start_id,
                        question_id end_id,
                        is_correct
                 from test_exam_question
                        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_question("Answer", "学生", "试题", relations)

    def sync_student_to_watch(self):
        sql = """
                         select user_id start_id,
                                chapter_id end_id,
                                position_sec progress
                         from user_chapter_progress
                                """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_watch("Watch", "学生", "章节", relations)





if __name__ == '__main__':
    sync = TableSynchronizer()
    # sync.sync_category()
    # sync.sync_subject()
    # sync.sync_chapter()
    # sync.sync_video()
    # sync.sync_test_paper()
    # sync.sync_test_paper_question()
    # sync.sync_course()
    # sync.sync_course_to_subject()
    # sync.sync_subject_to_category()
    # sync.sync_chapter_to_course()
    # sync.sync_video_to_chapter()
    # sync.sync_test_paper_to_course()
    # sync.sync_test_paper_question_to_test_paper()
    # sync.sync_student()
    # sync.sync_student_to_question()
    # sync.sync_student_to_course()
    # sync.sync_student_to_watch()