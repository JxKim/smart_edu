# 同步结构化数据

from utils import Utils

utils = Utils()

# 同步节点
# 大分类
def sync_category():
    # 在sql中读取数据
    result=utils.read_sql(table_name="base_category_info")
    # 数据清洗(当前节点不需要)
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="base_category_info",
                     label="category",
                     messages=["id","category_name"],
                     result=result)
# 学科
def sync_subject():
    # 在sql中读取数据
    result=utils.read_sql(table_name="base_subject_info")
    # 数据清洗(当前节点不需要)
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="base_subject_info",
                     label="subject",
                     messages=["id","subject_name"],
                     result=result)
# 用户
def sync_user():
    # 在sql中读取数据
    result=utils.read_sql(table_name="user_info")
    # 数据清洗--把性别未知的设置为<UNK>
    for index,res in enumerate(result):
        if result[index]["gender"] is None:
            result[index]["gender"]="<UNK>"

    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="user_info",
                     label="user",
                     messages=["id","login_name","nick_name","real_name","birthday","gender"],
                     result=result)
# 题目
def sync_question():
    # 在sql中读取数据
    result=utils.read_sql(table_name="test_question_info")
    # 数据清洗--如果问题字段过短,标记为噪声
    for index,res in enumerate(result):
        if len(result[index]["question_txt"]) <= 10:
            result[index]["question_txt"]="<NOISE>"

    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="test_question_info",
                     label="question",
                     messages=["id","question_txt"],
                     result=result)
# 知识点
def sync_knowledge_point():
    # 在sql中读取数据
    result = utils.read_sql(table_name="knowledge_point")
    # 数据清洗--先这样，后续可以调用大模型清洗（省钱小助手
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="knowledge_point",
                     label="knowledge_point",
                     messages=["id", "point_txt"],
                     result=result)
# 评价(核心字段全空！)
# 评论
def sync_comment_info():
    # 在sql中读取数据
    result = utils.read_sql(table_name="comment_info")
    # 数据清洗--全都是123 没法清洗
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="comment_info",
                     label="comment",
                     messages=["id", "comment_txt"],
                     result=result)
# 收藏
def sync_favor():
    # 在sql中读取数据
    result = utils.read_sql(table_name="favor_info")
    # 数据清洗--暂时不需要
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    # 只要id就行，后续会和别的节点建立关系
    utils.write_node(table_name="favor_info",
                     label="favor",
                     messages=["id"],
                     result=result)
# 章节进度（这里的秒数后面也可以处理掉）--这里可以提醒一下当前用户是否低于平均进度
def sync_chapter_progress():
    # 在sql中读取数据
    result = utils.read_sql(table_name="user_chapter_progress")
    # 数据清洗--暂时不需要
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="user_chapter_progress",
                     label="chapter_progress",
                     messages=["id","position_sec"],
                     result=result)
# 做题记录
def sync_question_record():
    # 在sql中读取数据
    result = utils.read_sql(table_name="test_exam_question")
    # 数据清洗(筛选回答中的空值，如果是空，当前的是否正确，分数)
    for index,res in enumerate(result):
        result[index]["score"]=float(result[index]["score"])
        if result[index]["answer"] is None:
            result[index]["answer"]="<MISSING>"
            result[index]["is_correct"]="<UNK>"
            result[index]["score"]=0.0
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="test_exam_question",
                     label="question_record",
                     messages=["id","answer","is_correct","score"],
                     result=result)
# 试卷test_paper
def sync_test_paper():
    # 在sql中读取数据
    result = utils.read_sql(table_name="test_paper")
    # 数据清洗--暂时不需要
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="test_paper",
                     label="paper",
                     messages=["id","paper_title"],
                     result=result)
# 题目选项(写入有点慢,可以考虑优化)
def sync_question_option():
    # 在sql中读取数据
    result = utils.read_sql(table_name="test_question_option")
    # 数据清洗--后续处理
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="test_question_option",
                     label="question_option",
                     messages=["id","option_txt","is_correct"],
                     result=result)
# chapter_info 章节
def sync_chapter():
    # 在sql中读取数据
    result = utils.read_sql(table_name="chapter_info")
    # 数据清洗--后续处理
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="test_question_option",
                     label="question_option",
                     messages=["id","chapter_name","is_free"],
                     result=result)
# video_info 视频
def sync_video():
    # 在sql中读取数据
    result = utils.read_sql(table_name="video_info")
    # 数据清洗--后续处理
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="video_info",
                     label="video",
                     messages=["id","video_name"],
                     result=result)
# course_info 课程
def sync_course_info():
    # 在sql中读取数据
    result = utils.read_sql(table_name="course_info")
    # 数据清洗--后续处理
    # 把清洗好的数据 每一个当做一个节点 放入到图数据库中
    utils.write_node(table_name="course_info",
                     label="course",
                     messages=["id","course_name","teacher","course_introduce"],
                     result=result)

# 批量同步大部分关系
def batch_sync_relation():
    # 批量参数的字典
    args_dict = [
        # 用户 - 观看 - 章节
        ["user_chapter_progress", "user", "chapter", "WATCHED", "user_id", "id"],
        # 用户 - 发布 - 评论
        ["comment_info", "user", "comment", "POSTED", "user_id", "id"],
        # 用户 - 收藏 - 课程
        ["favor_info", "user", "course", "COLLECTION", "user_id", "course_id"],
        # 用户 - 评论 - 课程
        ["comment_info", "user", "course", "RATED", "user_id", "course_id"],

        # 学科 - 属于 - 分类
        ["base_subject_info", "subject", "category", "BELONGS_TO", "id", "category_id"],

        # 学科 - 包含 - 课程
        ["course_info", "subject", "course", "HAS_COURSE", "subject_id", "id"],

        # 课程 - 有 - 视频
        ["video_info", "course", "video", "HAS_VIDEO", "course_id", "id"],
        # 课程 - 有 - 章节
        ["chapter_info", "course", "chapter", "HAS_CHAPTER", "course_id", "id"],
        # 章节 - 有 - 用户观看进度
        ["user_chapter_progress", "chapter", "chapter_progress", "HAS_CHAPTER", "chapter_id", "user_id"],
        # 章节 - 有 - 评论
        ["comment_info", "chapter", "comment", "HAS_COMMENT", "chapter_id", "id"],
        # 课程 - 有 - 题目
        ["test_question_info", "course", "question", "HAS_QUESTION", "course_id", "id"],
        # 题目 - 有 - 选项
        ["test_question_option", "question", "question_option", "HAS_OPTION", "question_id", "id"],
        # 试卷 - 有 - 题目
        ["test_paper_question", "paper", "question", "HAS_QUESTION", "paper_id", "question_id"],

        # 知识点 - 关联 - 题目
        ["test_point_question", "knowledge_point", "question", "RELATED_TO", "point_id", "question_id"],
        # 题目 - 属于 - 知识点
        ["test_point_question", "question", "knowledge_point", "COVERS", "question_id", "point_id"],
    ]

    for args in args_dict:
        utils.write_relation(*args)

if __name__=="__main__":
    batch_sync_relation()
    # 数据处理结束

    # # 最大分类节点
    # sync_category()
    # # 学科节点
    # sync_subject()
    # # 用户节点
    # sync_user()
    # # 题目
    # sync_question()
    # # 知识点
    # sync_knowledge_point()
    # # 评论
    # sync_comment_info()
    # # 收藏
    # sync_favor()
    # # 章节进度
    # sync_chapter_progress()
    # # 做题记录
    # sync_question_record()
    # # 试卷
    # sync_test_paper()
    # # 题目选项
    # sync_question_option()
    # # 章节
    # sync_chapter()

    # # 视频
    # sync_video()
    # # 课程
    # sync_course_info()



