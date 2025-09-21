from multiprocessing import synchronize
from datasync.utils import MysqlReader, Neo4jWriter

from decimal import Decimal




class TableSynchronizerlink:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
    
    def sync_base_subject_info_to_base_category_info(self):
        sql = """
              select id  start_id,
              category_id end_id
              from base_subject_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "base_subject_info", "base_category_info", relations)

    def sync_course_info_to_base_subject_info(self):
        sql = """
              select id  start_id,
              subject_id end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "course_info", "base_subject_info", relations)

    def synv_chapter_info_to_course_info(self):
        sql = """
              select id  start_id,
              course_id end_id
              from chapter_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "chapter_info", "course_info", relations)
    
    def sync_video_info_to_chapter_info(self):
        sql = """
              select id  start_id,
              chapter_id end_id
              from video_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "video_info", "chapter_info", relations)

    def sync_test_paper_to_course_info(self):
        sql = """
              select id  start_id,
              course_id end_id
              from test_paper
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "test_paper", "course_info", relations)

    def sync_test_paper_question_to_test_paper(self):
        sql = """
              select id  start_id,
              paper_id end_id
              from test_paper_question
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("belong", "test_paper_question", "test_paper", relations)

    def sync_teacher_to_course_info(self):
        sql = """
              select id start_id,
              id end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Have", "course_info","teacher",relations)

    def sync_price_to_course_info(self):
        sql = """
              select id start_id,
              id end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("Have", "course_info","price",relations)

    def sync_student_to_course_info(self):
        sql = """
              select user_id start_id,
              course_id end_id
              from order_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("BUY", "student","course_info",relations)

    def sync_student_to_update_time(self):
        sql = """
              select user_id start_id,
              update_time end_id
              from order_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("FAVOR", "student","update_time",relations)
if __name__ == '__main__':
    synchronizer = TableSynchronizerlink()
    # synchronizer.sync_base_subject_info_to_base_category_info()
    # synchronizer.sync_course_info_to_base_subject_info()
    # synchronizer.synv_chapter_info_to_course_info()
    # synchronizer.sync_video_info_to_chapter_info()
    # synchronizer.sync_test_paper_to_course_info()
    # synchronizer.sync_test_paper_question_to_test_paper()
    synchronizer.sync_teacher_to_course_info()
    synchronizer.sync_price_to_course_info()

