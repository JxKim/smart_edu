from datasync.utils_neo4j_mysql import MysqlReader, Neo4jWriter
import pymysql
from googletrans import Translator

class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.translator=Translator()
    # def sync_base_category_info(self):
    #     sql = """
    #           select *
    #           from base_category_info
    #           """
    #     properties = self.mysql_reader.read(sql)
    #
    #     self.neo4j_writer.write_nodes("base_category_info", properties)
    #
    # def sync_base_province(self):
    #     #area_code,iso_code,iso_3166_2
    #     sql = """
    #           select *
    #           from base_province
    #           """
    #     properties = self.mysql_reader.read(sql)
    #
    #     self.neo4j_writer.write_nodes("base_province", properties)
    #
    # def sync_base_source(self):
    #     #还有source_url
    #     sql = """
    #           select *
    #           from base_source
    #           """
    #     properties = self.mysql_reader.read(sql)
    #
    #     self.neo4j_writer.write_nodes("base_source", properties)
    #
    # def sync_base_subject_info(self):
    #     #create,update,deleted
    #     sql = """
    #           select *
    #           from base_subject_info
    #           """
    #     properties = self.mysql_reader.read(sql)
    #
    #     self.neo4j_writer.write_nodes("base_subject_info", properties)
    #
    # def sync_cart_info(self):
    #     sql = """
    #           select *
    #           from cart_info
    #           """
    #     properties = self.mysql_reader.read(sql)
    #
    #     self.neo4j_writer.write_nodes("cart_info", properties)

    def to_chinese(self,text: str) -> str:
        result = self.translator.translate(text, src='en', dest='zh-cn')
        return result.text

    def sync_node_batch(self,labels):
        for label in labels:
            sql = f"""
                  select *
                  from {label}
                  """
            properties = self.mysql_reader.read(sql)
            print(f'{label}')
            self.neo4j_writer.write_nodes(f"{label}", properties)
            print(f'{label}节点建立完毕')
            print('='*20)

    def sync_node_item_teacher(self):
        sql="""
        select distinct teacher
        from course_info
        """
        properties = self.mysql_reader.read(sql)
        print(properties)
        self.neo4j_writer.write_nodes_noid("teacher", properties)
        print('teacher节点建立完毕')

    def sync_relations_item_teacher(self):
        sql = """
              select teacher name,
                     id          end_id
              from course_info
              """
        relations = self.mysql_reader.read(sql)
        if relations:
            self.neo4j_writer.write_relations_teacher('Teach', 'teacher', 'course_info', relations)
        print('教师教课关系结束')
    def sync_relations_batch(self):
        relations_config = [
            # (关系类型, start表, start_id字段, end表, end_id字段)
            ('Belong', 'base_subject_info', 'category_id', 'base_category_info', 'id'),
            ('Owns', 'cart_info', 'user_id', 'user_info', 'id'),
            ('Contains', 'cart_info', 'course_id', 'course_info', 'id'),
            ('PartOf', 'chapter_info', 'course_id', 'course_info', 'id'),
            ('HasVideo', 'chapter_info', 'video_id', 'video_info', 'id'),
            ('PublishedBy', 'chapter_info', 'publisher_id', 'user_info', 'id'),
            ('CommentedBy', 'comment_info', 'user_id', 'user_info', 'id'),
            ('OnChapter', 'comment_info', 'chapter_id', 'chapter_info', 'id'),
            ('OnCourse', 'comment_info', 'course_id', 'course_info', 'id'),
            ('Belong', 'course_info', 'subject_id', 'base_subject_info', 'id'),
            ('PublishedBy', 'course_info', 'publisher_id', 'user_info', 'id'),
            ('FavorCourse', 'favor_info', 'course_id', 'course_info', 'id'),
            ('FavorBy', 'favor_info', 'user_id', 'user_info', 'id'),
            ('DetailOf', 'order_detail', 'order_id', 'order_info', 'id'),
            ('Contains', 'order_detail', 'course_id', 'course_info', 'id'),
            ('OrderedBy', 'order_detail', 'user_id', 'user_info', 'id'),
            ('BelongsTo', 'order_info', 'user_id', 'user_info', 'id'),
            ('FromProvince', 'order_info', 'province_id', 'base_province', 'id'),
            ('PaysFor', 'payment_info', 'order_id', 'order_info', 'id'),
            ('ReviewedBy', 'review_info', 'user_id', 'user_info', 'id'),
            ('ReviewFor', 'review_info', 'course_id', 'course_info', 'id'),
            ('ExamOf', 'test_exam', 'paper_id', 'test_paper', 'id'),
            ('TakenBy', 'test_exam', 'user_id', 'user_info', 'id'),
            ('PartOf', 'test_exam_question', 'exam_id', 'test_exam', 'id'),
            ('Question', 'test_exam_question', 'question_id', 'test_question_info', 'id'),
            ('ProgressOn', 'user_chapter_progress', 'course_id', 'course_info', 'id'),
            ('ProgressOnChapter', 'user_chapter_progress', 'chapter_id', 'chapter_info', 'id'),
            ('ProgressBy', 'user_chapter_progress', 'user_id', 'user_info', 'id'),
            ('BelongsToChapter', 'video_info', 'chapter_id', 'chapter_info', 'id'),
            ('BelongsToCourse', 'video_info', 'course_id', 'course_info', 'id'),
            ('PointOf', 'test_point_question', 'point_id', 'knowledge_point', 'id'),
            ('QuestionOf', 'test_point_question', 'question_id', 'test_question_info', 'id'),
            ('OptionOf', 'test_question_option', 'question_id', 'test_question_info', 'id'),
            ('PaperQuestion', 'test_paper_question', 'paper_id', 'test_paper', 'id'),
            ('QuestionInPaper', 'test_paper_question', 'question_id', 'test_question_info', 'id'),
            ('VipChangeOf', 'vip_change_detail', 'user_id', 'user_info', 'id')
        ]
        count=0
        for rel_type, start_table, start_field, end_table, end_field in relations_config:
            sql = f"SELECT id AS start_id, {start_field} AS end_id FROM {start_table} WHERE {start_field} IS NOT NULL"
            relations = self.mysql_reader.read(sql)
            if relations:
                self.neo4j_writer.write_relations(rel_type, start_table, end_table, relations)
                print(f'{count}:{rel_type}')
                count+=1
                print('='*20)

if __name__ == '__main__':

    #35585个节点创建完毕

    # connection = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456")
    # cursor = connection.cursor()
    # cursor.execute("""
    #     select
    #         t1.TABLE_NAME
    #     from
    #         information_schema.TABLES as t1
    #     where
    #         t1.TABLE_SCHEMA='ai_edu'
    # """)
    # results = cursor.fetchall()
    # labels = []
    # for table in results:
    #     table_name = table[0]
    #     labels.append(table_name)

    table_synchronizer = TableSynchronizer()


    # table_synchronizer.sync_node_batch(labels)
    #
    #
    # table_synchronizer.sync_relations_batch()


    # list=['base_subject_info','test_paper_question','user_info','Belong','Owns']
    # for item in list:
    #     listofword=[]
    #     listofword.extend(item.split('_'))
    #     for word in listofword:
    #         print(table_synchronizer.to_chinese(word))

    # table_synchronizer.sync_node_item_teacher()
    # table_synchronizer.sync_relations_item_teacher()


