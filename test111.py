import pymysql
from pymysql.cursors import DictCursor

from configuration import config

#
#
# connection = pymysql.connect(host="127.0.0.1",port=3306,user="root",password="123456")
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
# labels=[]
# for table in results:
#     table_name = table[0]
#     labels.append(table_name)
# print(labels)

#找到学科分类
# connection=pymysql.connect(**config.MYSQL_CONFIG)
# cursor=connection.cursor(DictCursor)
# cursor.execute("""
#     select subject_name
#     from base_subject_info
# """)
# results = cursor.fetchall()
# subject_list=[]
# for item in results:
#     subject_list.append(item['subject_name'])
# print(subject_list)


#找到知识点内容point_txt
# connection=pymysql.connect(**config.MYSQL_CONFIG)
# cursor=connection.cursor(DictCursor)
# cursor.execute("""
#     select point_txt
#     from knowledge_point
# """)
# results = cursor.fetchall()
# subject_list=[]
# for item in results:
#     subject_list.append(item['point_txt'])
# print(subject_list)

















# relations_config = [
#     # (关系类型, start表, start_id字段, end表, end_id字段)
#     ('Belong', 'base_subject_info', 'category_id', 'base_category_info', 'id'),
#     ('Owns', 'cart_info', 'user_id', 'user_info', 'id'),
#     ('Contains', 'cart_info', 'course_id', 'course_info', 'id'),
#     ('PartOf', 'chapter_info', 'course_id', 'course_info', 'id'),
#     ('HasVideo', 'chapter_info', 'video_id', 'video_info', 'id'),
#     ('PublishedBy', 'chapter_info', 'publisher_id', 'user_info', 'id'),
#     ('CommentedBy', 'comment_info', 'user_id', 'user_info', 'id'),
#     ('OnChapter', 'comment_info', 'chapter_id', 'chapter_info', 'id'),
#     ('OnCourse', 'comment_info', 'course_id', 'course_info', 'id'),
#     ('Belong', 'course_info', 'subject_id', 'base_subject_info', 'id'),
#     ('PublishedBy', 'course_info', 'publisher_id', 'user_info', 'id'),
#     ('FavorCourse', 'favor_info', 'course_id', 'course_info', 'id'),
#     ('FavorBy', 'favor_info', 'user_id', 'user_info', 'id'),
#     ('DetailOf', 'order_detail', 'order_id', 'order_info', 'id'),
#     ('Contains', 'order_detail', 'course_id', 'course_info', 'id'),
#     ('OrderedBy', 'order_detail', 'user_id', 'user_info', 'id'),
#     ('BelongsTo', 'order_info', 'user_id', 'user_info', 'id'),
#     ('FromProvince', 'order_info', 'province_id', 'base_province', 'id'),
#     ('PaysFor', 'payment_info', 'order_id', 'order_info', 'id'),
#     ('ReviewedBy', 'review_info', 'user_id', 'user_info', 'id'),
#     ('ReviewFor', 'review_info', 'course_id', 'course_info', 'id'),
#     ('ExamOf', 'test_exam', 'paper_id', 'test_paper', 'id'),
#     ('TakenBy', 'test_exam', 'user_id', 'user_info', 'id'),
#     ('PartOf', 'test_exam_question', 'exam_id', 'test_exam', 'id'),
#     ('Question', 'test_exam_question', 'question_id', 'test_question_info', 'id'),
#     ('ProgressOn', 'user_chapter_progress', 'course_id', 'course_info', 'id'),
#     ('ProgressOnChapter', 'user_chapter_progress', 'chapter_id', 'chapter_info', 'id'),
#     ('ProgressBy', 'user_chapter_progress', 'user_id', 'user_info', 'id'),
#     ('BelongsToChapter', 'video_info', 'chapter_id', 'chapter_info', 'id'),
#     ('BelongsToCourse', 'video_info', 'course_id', 'course_info', 'id'),
#     ('PointOf', 'test_point_question', 'point_id', 'knowledge_point', 'id'),
#     ('QuestionOf', 'test_point_question', 'question_id', 'test_question_info', 'id'),
#     ('OptionOf', 'test_question_option', 'question_id', 'test_question_info', 'id'),
#     ('PaperQuestion', 'test_paper_question', 'paper_id', 'test_paper', 'id'),
#     ('QuestionInPaper', 'test_paper_question', 'question_id', 'test_question_info', 'id'),
#     ('VipChangeOf', 'vip_change_detail', 'user_id', 'user_info', 'id')
# ]
# print(len(relations_config))