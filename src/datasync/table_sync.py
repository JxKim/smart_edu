from datasync.utils import MysqlReader,Neo4jWriter

class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()


    def sync_category(self):
        sql = """
                select id, category_name name
                 from base_category_info;
                """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Category", properties)

    def sync_subject(self):
        sql = """
                select id, subject_name name 
                 from base_subject_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Subject", properties)

    def sync_course(self):
        sql = """
                select id, course_name name 
                 from course_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Course", properties)

    def sync_teacher(self):
        sql = """
                select teacher name 
                 from course_info;
                 """
        properties = self.mysql_reader.read(sql)
        properties_set =set()
        for item in properties:
            properties_set.add(item["name"])
        properties_list = list(properties_set)
        properties = [{"id":index+1,"name":value} for index,value in enumerate(properties_list)]

        add_column_sql = """
        # ALTER TABLE course_info ADD COLUMN IF NOT EXISTS teacher_id INT;
        #             """
        # self.mysql_reader.write(add_column_sql)

        for teacher in properties:
            #print(teacher)
            update_sql = """    
            UPDATE course_info SET teacher_id = %s where teacher = %s
            """
            self.mysql_reader.write(update_sql,(teacher["id"],teacher["name"]))



        #print(properties)

        self.neo4j_writer.write_nodes("Teacher", properties)

    def sync_price(self):
        sql = """
                select id, cast(origin_price as char) name 
                 from course_info;
                 """
        properties = self.mysql_reader.read(sql)
        # properties_set = set()
        # for item in properties:
        #     properties_set.add(item["name"])
        # properties_list = list(properties_set)
        # properties = [{"id": index + 1, "name": str(value)} for index, value in enumerate(properties_list)]

        self.neo4j_writer.write_nodes("Price", properties)

    def sync_chapter(self):
        sql = """
                select id, chapter_name name 
                 from chapter_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Chapter", properties)

    def sync_video(self):
        sql = """
                select id, video_name name 
                 from video_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Video", properties)

    def sync_paper(self):
        sql = """
                select id, paper_title name 
                 from test_paper;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Paper", properties)

    def sync_question(self):
        sql = """
                select id, question_txt name 
                 from test_question_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("Question", properties)


    def sync_subject_to_category(self):
        sql = """
              select id   start_id,
                     category_id end_id
              from base_subject_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Subject", "Category", relations)

    def sync_course_to_subject(self):
        sql = """
              select id   start_id,
                     subject_id end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Course", "Subject", relations)

    def sync_chapter_to_course(self):
        sql = """
              select id   start_id,
                     course_id end_id
              from chapter_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Chapter", "Course", relations)

    def sync_video_to_chapter(self):
        sql = """
              select id   start_id,
                     chapter_id end_id
              from video_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Video", "Chapter", relations)

    def sync_paper_to_course(self):
        sql = """
              select id   start_id,
                     course_id end_id
              from test_paper
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Paper", "Course", relations)

    def sync_question_to_paper(self):
        sql = """
              select question_id   start_id,
                     paper_id end_id
              from test_paper_question
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Belong", "Chapter", "Course", relations)

    def sync_course_to_teacher(self):
        sql = """
              select c.id   start_id,
                     t.teacher_id   end_id
              from course_info c join course_info t
              on c.id = t.id
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Have", "Course", "Teacher", relations)

    def sync_course_to_price(self):
        sql = """
              select id   start_id,
                     id   end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Have", "Course", "Price", relations)





    #student_behavior


    def sync_Student(self):
        sql = """
                select id, real_name ,birthday, IFNULL(gender, 'UNK') gender
                 from user_info;
                 """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_info_nodes("Student", properties)

    def sync_student_to_course(self):
        sql = """
              select user_id   start_id,
                     course_id   end_id,
                     cast(create_time as char) favor_time
              from favor_info
              """
        relations = self.mysql_reader.read(sql)

        self.neo4j_writer.write_student_to_course_relations("Favor", "Student", "Course", relations)

    def sync_student_to_question(self):
        sql = """
              select user_id   start_id,
                     question_id   end_id,
                     is_correct
              from test_exam_question
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_to_question_relations("Answer", "Student", "Question", relations)


    def sync_student_to_video(self):
        sql = """              
                select u.user_id   start_id,
                       v.id   end_id,
                     position_sec progress,
                     u.create_time AS latest_time
                    from user_chapter_progress u join chapter_info c on u.chapter_id = c.id
                    join video_info v on c.video_id = v.id
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_student_to_video_relations("Watch", "Student", "Video", relations)




if __name__ == "__main__":
    table_synchronizer = TableSynchronizer()

    # table_synchronizer.sync_category()
    # table_synchronizer.sync_subject()
    # table_synchronizer.sync_course()
    # table_synchronizer.sync_teacher()
    # table_synchronizer.sync_price()
    # table_synchronizer.sync_chapter()
    # table_synchronizer.sync_video()
    # table_synchronizer.sync_paper()
    # table_synchronizer.sync_question()
    #
    # table_synchronizer.sync_subject_to_category()
    # table_synchronizer.sync_course_to_subject()
    # table_synchronizer.sync_chapter_to_course()
    # table_synchronizer.sync_video_to_chapter()
    # table_synchronizer.sync_paper_to_course()
    # table_synchronizer.sync_question_to_paper()
    # table_synchronizer.sync_course_to_teacher()
    # table_synchronizer.sync_course_to_price()
    #
    #
    #
    # table_synchronizer.sync_Student()
    # table_synchronizer.sync_student_to_course()
    # table_synchronizer.sync_student_to_question()
    table_synchronizer.sync_student_to_video()


