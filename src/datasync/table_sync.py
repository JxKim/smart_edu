from utils import MySqlReader,Neo4jWriter
class TableSync:
    def __init__(self):
        self.mysql_reader = MySqlReader()
        self.neo4j_writer = Neo4jWriter()
# 同步分类信息
    def sync_category_info(self):
        sql = """
            select id,
            category_name name
            from base_category_info
        """
        category = self.mysql_reader.read_data(sql)

        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("category", category)

#同步学科信息
    def sync_subject_info(self):
        sql = """
            select  category_id id,
                    subject_name name
            from base_subject_info
        """
        subject = self.mysql_reader.read_data(sql)


        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("subject", subject)
#同步科目和分类关系
    def subject_to_category(self):
        sql = """
            select  category_id end_id,
                    id start_id
            from base_subject_info
        """
        subject_to_category = self.mysql_reader.read_data(sql)

        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("category", "subject", "belong", subject_to_category)


    #同步学科和课程的关系
    def sync_course_to_subject(self):
        sql = """
            select  subject_id end_id,
                    id start_id
            from course_info
        """
        course_to_subject = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("course", "subject", "belong", course_to_subject)


    # 同步课程信息
    def sync_course_info(self):
        sql = """
            select  id,
                    course_name name
            from course_info
        """
        course = self.mysql_reader.read_data(sql)

        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("course", course)

    #同步章节信息
    def sync_chapter_info(self):
        sql = """
        select id,
            chapter_name name
        from chapter_info
        """
        chapter = self.mysql_reader.read_data(sql)

        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("chapter", chapter)

    #同步课程和章节关系
    def sync_course_to_chapter(self):
        sql = """
        select id end_id,
            course_id start_id
        from chapter_info
        """
        course_to_chapter = self.mysql_reader.read_data(sql)

        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("course", "chapter", "have", course_to_chapter)
        neo4j_writer.write_relations("chapter", "course", "belong", course_to_chapter)

    #同步试卷节点
    def sync_test_paper_info(self):
        sql = """
        select id,
            paper_title name
        from test_paper
        """
        test_paper = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("test_paper", test_paper)

    #同步试卷和课程关系
    def sync_test_paper_to_course(self):
        sql = """
        select course_id end_id,
            id start_id
        from test_paper
        """
        test_paper_to_course = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("course", "test_paper", "belong", test_paper_to_course)
        neo4j_writer.write_relations("test_paper", "course", "have", test_paper_to_course)


    #同步价格节点
    def sync_price_info(self):
        sql = """
        select id,
            FORMAT(actual_price, 2) name
        from course_info
        """
        price_info = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("price", price_info)

    def sync_course_to_price(self):

        cypher='''
        match (p:price),(c:course) 
        where p.id = c.id
        merge(p)-[:belong]->(c)
        merge(c)-[:have]->(p)
        '''
        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher)

    def sync_teacher_info(self):
        sql = """
        select id,
            teacher name
        from course_info
        """
        teacher = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("teacher", teacher)

    def sync_course_to_teacher(self):
        cypher = '''
                match (t:teacher),(c:course) 
                where t.id = c.id
                merge(t)-[:belong]->(c) 
                merge(c)-[:have]->(t) 
                
                '''

        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher)

    #同步章节视频的节点
    def sync_video_info(self):
        sql = """
        select id,
            video_name name
        from video_info
        """
        video = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("video", video)
    #同步章节和视频关系
    def sync_chapter_to_video(self):
        sql='''
        select id end_id,
        video_id start_id
        from chapter_info
        '''
        chapter_to_video = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("video", "chapter", "have", chapter_to_video)
        neo4j_writer.write_relations("chapter", "video", "belong", chapter_to_video)

    #同步试题节点
    def sync_question_info(self):
        sql = """
        select id,
        question_txt name
        from test_question_info
        """
        question = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_nodes("question", question)

    #同步试题属于哪个试卷
    def sync_question_to_paper(self):
        sql = """
        select question_id strat_id,
        paper_id end_id
        from test_paper_question 
        """
        question_to_paper = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.write_relations("question", "test_paper", "belong", question_to_paper)
        neo4j_writer.write_relations("test_paper", "question", "have", question_to_paper)

    #同步学生节点
    def sync_student_info(self):
        sql = """
        select id user_id,
        birthday,
        ifnull(gender,'unknown')as gender
        from user_info
        """

        cypher = f'''
                    unwind $batch as item
                    merge (n:student {{id:item.user_id,birthday:item.birthday,gender:item.gender}})
                '''

        student = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher, batch=student)

    #同步学生收藏哪些课程
    def sync_student_favor_to_course(self):
        sql = """
        select course_id end_id,
        user_id start_id,
        create_time time
        from favor_info
        """
        cypher = f'''
                    unwind $batch as item
                    match (s:student {{id:item.start_id}}),(c:course {{id:item.end_id}})
                    merge (s)-[:favor{{time:item.time}}]->(c)
                '''
        student_favor_to_course = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher, batch=student_favor_to_course)

    #同步学生做题记录
    def sync_student_do_question(self):
        sql = """
        select user_id start_id,
        question_id end_id,
        is_correct
        from test_exam_question
        """
        cypher = f'''
                    unwind $batch as item
                    match (s:student {{id:item.start_id}}),(q:question {{id:item.end_id}})
                    merge (s)-[:answer{{is_correct:item.is_correct}}]->(q)
                  '''
        student_to_question = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher, batch=student_to_question)
    #同步学生观看视频的进度
    def sync_student_to_chapter(self):
        sql = """
        select user_id start_id,
        chapter_id end_id,
        position_sec progress,
        create_time latest_watching_time
        from user_chapter_progress
        """
        cypher=f'''
                    unwind $batch as item
                    match (s:student {{id:item.start_id}}),(c:chapter {{id:item.end_id}})
                    merge (s)-[:watch{{progress:item.progress,latest_watching_time:item.latest_watching_time}}]->(c)
                    
                '''
        student_to_video = self.mysql_reader.read_data(sql)
        neo4j_writer = Neo4jWriter()
        neo4j_writer.driver.execute_query(cypher, batch=student_to_video)



if __name__ == '__main__':
    table_sync = TableSync()
    # table_sync.sync_category_info()
    # table_sync.sync_subject_info()
    table_sync.subject_to_category()
    table_sync.sync_course_to_subject()
    # table_sync.sync_course_info()
    # table_sync.sync_chapter_info()
    table_sync.sync_course_to_chapter()
    # table_sync.sync_test_paper_info()
    table_sync.sync_test_paper_to_course()
    # table_sync.sync_price_info()
    table_sync.sync_course_to_price()
    # table_sync.sync_teacher_info()
    table_sync.sync_course_to_teacher()
    # table_sync.sync_video_info()
    table_sync.sync_chapter_to_video()
    # table_sync.sync_question_info()
    table_sync.sync_question_to_paper()
    # table_sync.sync_student_info()
    # table_sync.sync_student_favor_to_course()
    # table_sync.sync_student_do_question()
    # table_sync.sync_student_to_chapter()


