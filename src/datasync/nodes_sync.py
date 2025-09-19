from datasync.utils import MysqlReader, Neo4jWriter


class NodesSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 分类
    def synchronize_category(self):
        sql = """
            select 
                id,
                category_name name
            from base_category_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Category',property)

    # 学科
    def synchronize_subject(self):
        sql = """
            select 
                id,
                subject_name name
            from base_subject_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Subject',property)

    # 课程----课程name有点长，是否需要截取？
    def synchronize_course(self):
        sql = """
            select 
                id,
                course_name name
            from course_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Course',property)
    # 教师
    def synchronize_teacher(self):
        sql = """
            select 
                id,
                teacher name
            from course_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Teacher',property)
    # 价格
    def synchronize_price(self):
        sql = """
            select 
                id,
                CAST(actual_price AS CHAR) name
            from course_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Price',property)

    # todo 章节----name有点怪，需要做数据清洗再写入？
    def synchronize_chapter(self):
        sql = """
            select 
                id,
                chapter_name name
            from chapter_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Chapter',property)


    # 视频
    def synchronize_video(self):
        sql = """
            select 
                id,
                video_name name
            from video_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Video',property)

    # 试卷
    def synchronize_paper(self):
        sql = """
            select 
                id,
                paper_title name
            from test_paper
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Paper',property)
    # 试题
    def synchronize_question(self):
        sql = """
            select 
                id,
                question_txt name
            from test_question_info
        """
        property = self.mysql_reader.read(sql)
        self.neo4j_writer.writer_nodes('Question',property)

    # 学生
    def synchronize_student(self):
        sql = """
            select 
                id uid,
                DATE_FORMAT(birthday, '%Y-%m-%d') birthday,
                CASE 
                    WHEN gender = 'M' THEN '女'
                    WHEN gender = 'F' THEN '男'
                    ELSE '未知'
                    END gender
            from user_info
        """
        property = self.mysql_reader.read(sql)
        # print( property)
        self.neo4j_writer.writer_nodes_with_three_properties('Student',property)

if __name__ == '__main__':
    nodes_synchronizer = NodesSynchronizer()
    # nodes_synchronizer.synchronize_category()
    # nodes_synchronizer.synchronize_subject()
    # nodes_synchronizer.synchronize_course()
    # nodes_synchronizer.synchronize_teacher()
    # nodes_synchronizer.synchronize_price()
    # nodes_synchronizer.synchronize_chapter()

    # nodes_synchronizer.synchronize_video()
    # nodes_synchronizer.synchronize_paper()
    # nodes_synchronizer.synchronize_question()

    nodes_synchronizer.synchronize_student()
