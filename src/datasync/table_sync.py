from multiprocessing import synchronize
from datasync.utils import MysqlReader, Neo4jWriter

from decimal import Decimal

def convert_decimal(obj):
    """把字典中所有 decimal.Decimal 转为 float"""
    for item in obj:
        for k, v in item.items():
            if isinstance(v, Decimal):
                item[k] = float(v)
    return obj


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def get_table_columns(self, table_name):
        sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'ai_edu'
              AND TABLE_NAME = '{table_name}';
        """
        columns = self.mysql_reader.read(sql)
        return [col['COLUMN_NAME'] for col in columns]



class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def get_table_columns(self, table_name):
        sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'ai_edu'
              AND TABLE_NAME = '{table_name}';
        """
        columns = self.mysql_reader.read(sql)
        return [col['COLUMN_NAME'] for col in columns]

    # ===========================
    # 自动同步 MySQL 表到 Neo4j
    # ===========================

    def sync_base_category_info(self):
        columns = self.get_table_columns("base_category_info")
        sql = f"SELECT {','.join(columns)} FROM base_category_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("base_category_info", properties)

    def sync_base_province(self):
        columns = self.get_table_columns("base_province")
        sql = f"SELECT {','.join(columns)} FROM base_province"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("base_province", properties)

    def sync_base_source(self):
        columns = self.get_table_columns("base_source")
        sql = f"SELECT {','.join(columns)} FROM base_source"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("base_source", properties)

    def sync_base_subject_info(self):
        columns = self.get_table_columns("base_subject_info")
        sql = f"SELECT {','.join(columns)} FROM base_subject_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("base_subject_info", properties)

    def sync_cart_info(self):
        columns = self.get_table_columns("cart_info")
        sql = f"SELECT {','.join(columns)} FROM cart_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("cart_info", properties)

    def sync_chapter_info(self):
        columns = self.get_table_columns("chapter_info")
        sql = f"SELECT {','.join(columns)} FROM chapter_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("chapter_info", properties)

    def sync_comment_info(self):
        columns = self.get_table_columns("comment_info")
        sql = f"SELECT {','.join(columns)} FROM comment_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("comment_info", properties)

    def sync_course_info(self):
        columns = self.get_table_columns("course_info")
        sql = f"SELECT {','.join(columns)} FROM course_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("course_info", properties)

    def sync_favor_info(self):
        columns = self.get_table_columns("favor_info")
        sql = f"SELECT {','.join(columns)} FROM favor_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("favor_info", properties)

    def sync_knowledge_point(self):
        columns = self.get_table_columns("knowledge_point")
        sql = f"SELECT {','.join(columns)} FROM knowledge_point"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("knowledge_point", properties)

    def sync_order_detail(self):
        columns = self.get_table_columns("order_detail")
        sql = f"SELECT {','.join(columns)} FROM order_detail"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("order_detail", properties)

    def sync_order_info(self):
        columns = self.get_table_columns("order_info")
        sql = f"SELECT {','.join(columns)} FROM order_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("order_info", properties)

    def sync_payment_info(self):
        columns = self.get_table_columns("payment_info")
        sql = f"SELECT {','.join(columns)} FROM payment_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("payment_info", properties)

    def sync_review_info(self):
        columns = self.get_table_columns("review_info")
        sql = f"SELECT {','.join(columns)} FROM review_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("review_info", properties)

    def sync_test_exam(self):
        columns = self.get_table_columns("test_exam")
        sql = f"SELECT {','.join(columns)} FROM test_exam"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_exam", properties)

    def sync_test_exam_question(self):
        columns = self.get_table_columns("test_exam_question")
        sql = f"SELECT {','.join(columns)} FROM test_exam_question"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_exam_question", properties)

    def sync_test_paper(self):
        columns = self.get_table_columns("test_paper")
        sql = f"SELECT {','.join(columns)} FROM test_paper"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_paper", properties)

    def sync_test_paper_question(self):
        columns = self.get_table_columns("test_paper_question")
        sql = f"SELECT {','.join(columns)} FROM test_paper_question"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_paper_question", properties)

    def sync_test_point_question(self):
        columns = self.get_table_columns("test_point_question")
        sql = f"SELECT {','.join(columns)} FROM test_point_question"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_point_question", properties)

    def sync_test_question_info(self):
        columns = self.get_table_columns("test_question_info")
        sql = f"SELECT {','.join(columns)} FROM test_question_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_question_info", properties)

    def sync_test_question_option(self):
        columns = self.get_table_columns("test_question_option")
        sql = f"SELECT {','.join(columns)} FROM test_question_option"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("test_question_option", properties)

    def sync_user_chapter_progress(self):
        columns = self.get_table_columns("user_chapter_progress")
        sql = f"SELECT {','.join(columns)} FROM user_chapter_progress"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("user_chapter_progress", properties)

    def sync_user_info(self):
        columns = self.get_table_columns("user_info")
        sql = f"SELECT {','.join(columns)} FROM user_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("user_info", properties)

    def sync_video_info(self):
        columns = self.get_table_columns("video_info")
        sql = f"SELECT {','.join(columns)} FROM video_info"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("video_info", properties)

    def sync_vip_change_detail(self):
        columns = self.get_table_columns("vip_change_detail")
        sql = f"SELECT {','.join(columns)} FROM vip_change_detail"
        properties = self.mysql_reader.read(sql)
        properties = convert_decimal(properties)
        self.neo4j_writer.write_nodes("vip_change_detail", properties)
    
    def sync_teacher(self):
        sql = """
           select  id ,
                  teacher
            from course_info"""
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("teacher", properties)
    
    def sync_price(self):
        sql = """
           select  id ,
                  actual_price
            from course_info"""
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("price", properties)

    def sync_student(self):
        sql = """
            select  id,
                   real_name,
                   gender,
                   birthday
            from user_info"""
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("student", properties)

    def sync_update_time(self):
        sql ="""
            select user_id,
             create_time
             from favor_info"""
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes2("create_timee", properties)

    def sync_is_correct(self):
        sql = """
            select user_id,
                   is_correct
            from test_exam_question"""
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("is_correct", properties)
if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    # # 同步所有基础数据
    # synchronizer.sync_base_category_info()
    # synchronizer.sync_base_province()
    # synchronizer.sync_base_source()
    # synchronizer.sync_base_subject_info()

    # # 同步课程和章节相关
    # synchronizer.sync_cart_info()
    # synchronizer.sync_chapter_info()
    # synchronizer.sync_comment_info()
    # synchronizer.sync_course_info()

    # # 同步偏好和知识点
    # synchronizer.sync_favor_info()
    # synchronizer.sync_knowledge_point()

    # # 同步订单相关
    # synchronizer.sync_order_detail()
    # synchronizer.sync_order_info()
    # synchronizer.sync_payment_info()
    # synchronizer.sync_review_info()

    # # 同步考试相关
    # synchronizer.sync_test_exam()
    # synchronizer.sync_test_exam_question()
    # synchronizer.sync_test_paper()
    # synchronizer.sync_test_paper_question()
    # synchronizer.sync_test_point_question()
    # synchronizer.sync_test_question_info()
    # synchronizer.sync_test_question_option()

    # # 同步用户和进度信息
    # synchronizer.sync_user_chapter_progress()
    # synchronizer.sync_user_info()

    # # 同步视频和 VIP 信息
    # synchronizer.sync_video_info()
    # synchronizer.sync_vip_change_detail()
    # synchronizer.sync_teacher()
    # synchronizer.sync_price()
    synchronizer.sync_student()
    synchronizer.sync_update_time()
    