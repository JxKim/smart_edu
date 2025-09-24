from datasync.utils import Neo4jWriter,MySqlReader

class TableSynchronizer:
    def __init__(self):
        self.reader = MySqlReader()
        self.writer=Neo4jWriter()

    def start(self):
        sql = '''
               select id,category_name as name
               from base_category_info
               '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Base_category', properties)

        sql = '''
                   select id,subject_name as name
                   from base_subject_info
                   '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Base_subject', properties)

        sql = '''
                           select id,course_name as name
                           from course_info
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Course', properties)

        sql = '''
                           select id,paper_title as name
                           from test_paper
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Test_paper', properties)

        sql = '''
                           select id,question_txt as name
                           from test_question_info
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Test_question', properties)

        sql = '''
                           select id,real_name as name
                           from user_info
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('User', properties)

        sql = '''
                           select id,point_txt as name
                           from knowledge_point
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Knowledge_point', properties)

        sql = '''
                           select id,chapter_name as name
                           from chapter_info
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Chapter', properties)

        sql = '''
                           select id,video_name as name
                           from video_info
                           '''
        properties = self.reader.read(sql)
        self.writer.write_nodes('Video', properties)

        sql = '''
                                   select id,origin_price,actual_price,teacher
                                   from course_info
                                   '''
        properties = self.reader.read(sql)
        self.writer.add_properties('Course', properties)

        sql = '''
                       select a.id as start_id,b.id as end_id
                       from base_subject_info as a join base_category_info as b on a.category_id = b.id 
                       '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Belong', 'Base_subject', 'Base_category', relations)

        sql = '''
                               select a.id as start_id,b.id as end_id
                               from course_info as a join base_subject_info as b on a.subject_id = b.id 
                               '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Belong', 'Course', 'Base_subject', relations)

        sql = '''
                                       select a.id as start_id,b.id as end_id
                                       from test_paper as a join course_info as b on a.course_id = b.id 
                                       '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Belong', 'Test_paper', 'Course', relations)

        sql = '''
                                               select a.id as start_id,b.id as end_id
                                               from chapter_info as a join course_info as b on a.course_id = b.id 
                                               '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Belong', 'Chapter', 'Course', relations)

        sql = '''
                                                       select a.id as start_id,b.id as end_id
                                                       from video_info as a join chapter_info as b on a.chapter_id = b.id 
                                                       '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Belong', 'Video', 'Chapter', relations)

        sql = '''
                                                               select a.paper_id as start_id,a.question_id as end_id
                                                               from test_paper_question as a
                                                               '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Have', 'Test_paper', 'Test_question', relations)

        sql = '''
                                                                       select a.user_id as start_id,a.question_id as end_id
                                                                       from test_exam_question as a
                                                                       '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Answer', 'User', 'Test_question', relations)

        sql = '''
                                                                               select a.user_id as start_id,a.question_id as end_id,a.is_correct
                                                                               from test_exam_question as a
                                                                               '''
        relation_properties = self.reader.read(sql)
        self.writer.add_relations_properties('Answer', 'User', 'Test_question', relation_properties)

        sql = '''
                                                                       select a.question_id as start_id,a.point_id as end_id
                                                                       from test_point_question as a
                                                                       '''
        relations = self.reader.read(sql)
        self.writer.write_relations('Have', 'Test_question', 'Knowledge_point', relations)

if __name__ == '__main__':
    tablesynchronizer = TableSynchronizer()
    tablesynchronizer.start()





