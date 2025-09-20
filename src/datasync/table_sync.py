from datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    def base_category_info(self):
        sql = """
              select id, category_name, create_time, update_time, deleted
              from base_category_info
              where deleted != '1'  -- 筛选未删除数据
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("base_category_info", properties)

    def base_province(self):
        sql = """
              select id, name, region_id, area_code, iso_code, iso_3166_2
              from base_province
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("base_province", properties)

    def base_source(self):
        sql = """
              select id, source_site, source_url
              from base_source
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("base_source", properties)

    def base_subject_info(self):
        sql = """
              select id, subject_name, category_id, create_time, update_time, deleted
              from base_subject_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("base_subject_info", properties)

    def cart_info(self):
        sql = """
              select id, user_id, course_id, course_name, cart_price, img_url, session_id, create_time, update_time, deleted, sold
              from cart_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("cart_info", properties)

    def chapter_info(self):
        sql = """
              select id, chapter_name, course_id, video_id, publisher_id, is_free, create_time, deleted, update_time
              from chapter_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("chapter_info", properties)

    def comment_info(self):
        sql = """
              select id, user_id, chapter_id, course_id, comment_txt, create_time, deleted
              from comment_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("comment_info", properties)

    def course_info(self):
        sql = """
              select id, course_name, course_slogan, course_cover_url, subject_id, teacher, publisher_id, chapter_num, origin_price, reduce_amount, actual_price, course_introduce, create_time, deleted, update_time
              from course_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("course_info", properties)

    def favor_info(self):
        sql = """
              select id, course_id, user_id, create_time, update_time, deleted
              from favor_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("favor_info", properties)

    def knowledge_point(self):
        sql = """
              select id, point_txt, create_time, update_time, publisher_id, deleted
              from knowledge_point
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("knowledge_point", properties)

    def order_detail(self):
        sql = """
              select id, course_id, course_name, order_id, user_id, origin_amount, coupon_reduce, final_amount, create_time, update_time
              from order_detail
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("order_detail", properties)

    def order_info(self):
        sql = """
              select id, user_id, origin_amount, coupon_reduce, final_amount, order_status, out_trade_no, trade_body, session_id, province_id, create_time, expire_time, update_time
              from order_info
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("order_info", properties)

    def payment_info(self):
        sql = """
              select id, out_trade_no, order_id, alipay_trade_no, total_amount, trade_body, payment_type, payment_status, create_time, update_time, callback_content, callback_time
              from payment_info
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("payment_info", properties)

    def review_info(self):
        sql = """
              select id, user_id, course_id, review_txt, review_stars, create_time, deleted
              from review_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("review_info", properties)

    def test_exam(self):
        sql = """
              select id, paper_id, user_id, score, duration_sec, create_time, submit_time, update_time, deleted
              from test_exam
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_exam", properties)

    def test_exam_question(self):
        sql = """
              select id, exam_id, paper_id, question_id, user_id, answer, is_correct, score, create_time, update_time, deleted
              from test_exam_question
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_exam_question", properties)

    def test_paper(self):
        sql = """
              select id, paper_title, course_id, create_time, update_time, publisher_id, deleted
              from test_paper
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_paper", properties)

    def test_paper_question(self):
        sql = """
              select id, paper_id, question_id, score, create_time, deleted, publisher_id
              from test_paper_question
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_paper_question", properties)

    def test_point_question(self):
        sql = """
              select id, point_id, question_id, create_time, publisher_id, deleted
              from test_point_question
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_point_question", properties)

    def test_question_info(self):
        sql = """
              select id, question_txt, category_id, course_id, question_type, create_time, update_time, publisher_id, deleted
              from test_question_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_question_info", properties)

    def test_question_option(self):
        sql = """
              select id, option_txt, question_id, is_correct, create_time, update_time, deleted
              from test_question_option
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("test_question_option", properties)

    def user_chapter_progress(self):
        sql = """
              select id, course_id, chapter_id, user_id, position_sec, create_time, update_time, deleted
              from user_chapter_progress
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("user_chapter_progress", properties)

    def user_info(self):
        sql = """
              select id, login_name, nick_name, passwd, real_name, phone_num, email, head_img, user_level, birthday, gender, create_time, operate_time, status
              from user_info
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("user_info", properties)

    def video_info(self):
        sql = """
              select id, video_name, during_sec, video_status, video_size, video_url, video_source_id, version_id, chapter_id, course_id, publisher_id, create_time, update_time, deleted
              from video_info
              where deleted != '1'
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("video_info", properties)

    def vip_change_detail(self):
        sql = """
              select id, user_id, from_vip, to_vip, create_time
              from vip_change_detail
              """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_nodes("vip_change_detail", properties)

    def sync_subject_to_category(self):
        # 学科属于分类
        sql = """
              select id     start_id,
                     category_id end_id
              from base_subject_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        # 使用表名作为节点标签，与节点同步保持一致
        self.neo4j_writer.write_relations(
            "BelongTo",
            "base_subject_info",
            "base_category_info",
            relations
        )

    def sync_course_to_subject(self):
        # 课程属于学科
        sql = """
              select id     start_id,
                     subject_id end_id
              from course_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "course_info",
            "base_subject_info",
            relations
        )

    def sync_chapter_to_course(self):
        # 章节属于课程
        sql = """
              select id     start_id,
                     course_id end_id
              from chapter_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "chapter_info",
            "course_info",
            relations
        )

    def sync_video_to_chapter(self):
        # 视频属于章节
        sql = """
              select id     start_id,
                     chapter_id end_id
              from video_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "video_info",
            "chapter_info",
            relations
        )

    def sync_cart_to_user(self):
        # 购物车属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from cart_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "cart_info",
            "user_info",
            relations
        )

    def sync_cart_to_course(self):
        # 购物车包含课程
        sql = """
              select id     start_id,
                     course_id end_id
              from cart_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Contain",
            "cart_info",
            "course_info",
            relations
        )

    def sync_comment_to_user(self):
        # 评论属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from comment_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Make",
            "comment_info",
            "user_info",
            relations
        )

    def sync_comment_to_chapter(self):
        # 评论针对章节
        sql = """
              select id     start_id,
                     chapter_id end_id
              from comment_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Target",
            "comment_info",
            "chapter_info",
            relations
        )

    def sync_favor_to_user(self):
        # 收藏属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from favor_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Make",
            "favor_info",
            "user_info",
            relations
        )

    def sync_favor_to_course(self):
        # 收藏针对课程
        sql = """
              select id     start_id,
                     course_id end_id
              from favor_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Target",
            "favor_info",
            "course_info",
            relations
        )

    def sync_order_to_user(self):
        # 订单属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from order_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Place",
            "order_info",
            "user_info",
            relations
        )

    def sync_order_to_province(self):
        # 订单关联省份
        sql = """
              select id     start_id,
                     province_id end_id
              from order_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "From",
            "order_info",
            "base_province",
            relations
        )

    def sync_order_detail_to_order(self):
        # 订单明细属于订单
        sql = """
              select id     start_id,
                     order_id end_id
              from order_detail
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "order_detail",
            "order_info",
            relations
        )

    def sync_order_detail_to_course(self):
        # 订单明细包含课程
        sql = """
              select id     start_id,
                     course_id end_id
              from order_detail
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Include",
            "order_detail",
            "course_info",
            relations
        )

    def sync_payment_to_order(self):
        # 支付信息关联订单
        sql = """
              select id     start_id,
                     order_id end_id
              from payment_info
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "PayFor",
            "payment_info",
            "order_info",
            relations
        )

    def sync_review_to_user(self):
        # 评价属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from review_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Make",
            "review_info",
            "user_info",
            relations
        )

    def sync_review_to_course(self):
        # 评价针对课程
        sql = """
              select id     start_id,
                     course_id end_id
              from review_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Target",
            "review_info",
            "course_info",
            relations
        )

    def sync_exam_to_paper(self):
        # 考试记录关联试卷
        sql = """
              select id     start_id,
                     paper_id end_id
              from test_exam
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Use",
            "test_exam",
            "test_paper",
            relations
        )

    def sync_exam_to_user(self):
        # 考试记录属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from test_exam
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Take",
            "test_exam",
            "user_info",
            relations
        )

    def sync_exam_question_to_exam(self):
        # 试题记录属于考试
        sql = """
              select id     start_id,
                     exam_id end_id
              from test_exam_question
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "test_exam_question",
            "test_exam",
            relations
        )

    def sync_exam_question_to_question(self):
        # 试题记录关联试题
        sql = """
              select id     start_id,
                     question_id end_id
              from test_exam_question
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Answer",
            "test_exam_question",
            "test_question_info",
            relations
        )

    def sync_paper_to_course(self):
        # 试卷属于课程
        sql = """
              select id     start_id,
                     course_id end_id
              from test_paper
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "test_paper",
            "course_info",
            relations
        )

    def sync_paper_question_to_paper(self):
        # 试卷试题关联试卷
        sql = """
              select id     start_id,
                     paper_id end_id
              from test_paper_question
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "Contain",
            "test_paper_question",
            "test_paper",
            relations
        )

    def sync_point_question_to_point(self):
        # 知识点试题关联知识点
        sql = """
              select id     start_id,
                     point_id end_id
              from test_point_question
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "RelateTo",
            "test_point_question",
            "knowledge_point",
            relations
        )

    def sync_question_to_category(self):
        # 试题属于分类
        sql = """
              select id     start_id,
                     category_id end_id
              from test_question_info
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "BelongTo",
            "test_question_info",
            "base_category_info",
            relations
        )

    def sync_question_option_to_question(self):
        # 试题选项属于试题
        sql = """
              select id     start_id,
                     question_id end_id
              from test_question_option
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "HaveOption",
            "test_question_option",
            "test_question_info",
            relations
        )

    def sync_user_progress_to_user(self):
        # 用户进度属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from user_chapter_progress
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "HaveProgress",
            "user_chapter_progress",
            "user_info",
            relations
        )

    def sync_user_progress_to_course(self):
        # 用户进度关联课程
        sql = """
              select id     start_id,
                     course_id end_id
              from user_chapter_progress
              where deleted != '1'
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "ProgressOn",
            "user_chapter_progress",
            "course_info",
            relations
        )

    def sync_vip_change_to_user(self):
        # VIP变更属于用户
        sql = """
              select id     start_id,
                     user_id end_id
              from vip_change_detail
              """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations(
            "ChangeVIP",
            "vip_change_detail",
            "user_info",
            relations
        )


if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    # 同步所有节点表数据
    # synchronizer.base_category_info()
    # synchronizer.base_province()
    # synchronizer.base_source()
    # synchronizer.base_subject_info()
    # synchronizer.cart_info()
    # synchronizer.chapter_info()
    # synchronizer.comment_info()
    # synchronizer.course_info()
    # synchronizer.favor_info()
    # synchronizer.knowledge_point()
    # synchronizer.order_detail()
    # synchronizer.order_info()
    # synchronizer.payment_info()
    # synchronizer.review_info()
    # synchronizer.test_exam()
    # synchronizer.test_exam_question()
    # synchronizer.test_paper()
    # synchronizer.test_paper_question()
    # synchronizer.test_point_question()
    # synchronizer.test_question_info()
    # synchronizer.test_question_option()
    # synchronizer.user_chapter_progress()
    # synchronizer.user_info()
    # synchronizer.video_info()
    # synchronizer.vip_change_detail()

    # 同步所有关系数据（建议在节点同步完成后执行）
    synchronizer.sync_subject_to_category()
    synchronizer.sync_course_to_subject()
    synchronizer.sync_chapter_to_course()
    synchronizer.sync_video_to_chapter()
    synchronizer.sync_cart_to_user()
    synchronizer.sync_cart_to_course()
    synchronizer.sync_comment_to_user()
    synchronizer.sync_comment_to_chapter()
    synchronizer.sync_favor_to_user()
    synchronizer.sync_favor_to_course()
    synchronizer.sync_order_to_user()
    synchronizer.sync_order_to_province()
    synchronizer.sync_order_detail_to_order()
    synchronizer.sync_order_detail_to_course()
    synchronizer.sync_payment_to_order()
    synchronizer.sync_review_to_user()
    synchronizer.sync_review_to_course()
    synchronizer.sync_exam_to_paper()
    synchronizer.sync_exam_to_user()
    synchronizer.sync_exam_question_to_exam()
    synchronizer.sync_exam_question_to_question()
    synchronizer.sync_paper_to_course()
    synchronizer.sync_paper_question_to_paper()
    synchronizer.sync_point_question_to_point()
    synchronizer.sync_question_to_category()
    synchronizer.sync_question_option_to_question()
    synchronizer.sync_user_progress_to_user()
    synchronizer.sync_user_progress_to_course()
    synchronizer.sync_vip_change_to_user()

