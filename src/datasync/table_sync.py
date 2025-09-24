# migrate_data.py
from utils_data import MysqlReader , Neo4jWriter
from configuration import config
import time


def migrate_course_data ( mysql_reader , neo4j_writer ) :
    """迁移课程数据"""
    print ( "开始迁移课程数据..." )

    # 1. 导入分类
    categories = mysql_reader.read ( "SELECT id as category_id, category_name as name FROM base_category_info" )
    neo4j_writer.write_nodes ( "分类" , "category_id" , categories )

    # 2. 导入学科
    subjects = mysql_reader.read ( "SELECT id as subject_id, subject_name as name, category_id FROM base_subject_info" )
    neo4j_writer.write_nodes ( "学科" , "subject_id" , subjects )

    # 3. 导入课程（修改部分：转换价格字段类型）
    courses = mysql_reader.read ( """
        SELECT id as course_id, course_name as name, course_slogan, course_cover_url, 
               subject_id,  chapter_num, origin_price, reduce_amount, 
               actual_price, course_introduce
        FROM course_info
    """ )

    # 转换Decimal类型字段为float
    for course in courses :
        # 处理可能为None的情况，转换为float
        course['origin_price'] = float ( course['origin_price'] ) if course['origin_price'] is not None else None
        course['reduce_amount'] = float ( course['reduce_amount'] ) if course['reduce_amount'] is not None else None
        course['actual_price'] = float ( course['actual_price'] ) if course['actual_price'] is not None else None

    neo4j_writer.write_nodes ( "课程" , "course_id" , courses )

    # 4. 导入教师（从课程信息中提取）
    teachers_name = ["张老师","赵老师","魏老师","刘老师","付老师","李老师"]
    teachers = []

    for teacher_name in teachers_name:
        teachers.append ( {"teacher_name": teacher_name})
    # 写入Neo4j
    neo4j_writer.write_nodes ( "教师" , "teacher_name" , teachers )

    # 5. 导入价格信息（同样处理Decimal类型）
    prices = []
    for course in courses :
        prices.append ( {
            "price_id" : f"price_{course['course_id']}" ,
            # 转换Decimal为float，处理None值
            "origin_price" : float ( course.get ( 'origin_price' ) ) if course.get (
                'origin_price' ) is not None else None ,
            "reduce_amount" : float ( course.get ( 'reduce_amount' ) ) if course.get (
                'reduce_amount' ) is not None else None ,
            "actual_price" : float ( course.get ( 'actual_price' ) ) if course.get (
                'actual_price' ) is not None else None
        } )
    neo4j_writer.write_nodes ( "价格" , "price_id" , prices )

    # 6. 导入章节
    chapters = mysql_reader.read (
        "SELECT id as chapter_id, chapter_name as name, course_id, is_free FROM chapter_info" )
    neo4j_writer.write_nodes ( "章节" , "chapter_id" , chapters )

    # 7. 导入视频
    videos = mysql_reader.read ( """
        SELECT id as video_id, video_name as name, during_sec, video_url, chapter_id
        FROM video_info
    """ )
    neo4j_writer.write_nodes ( "视频" , "video_id" , videos )

    # 8. 导入试卷
    papers = mysql_reader.read ( "SELECT id as paper_id, paper_title as name, course_id FROM test_paper" )
    neo4j_writer.write_nodes ( "试卷" , "paper_id" , papers )

    # 9. 导入试题
    questions = mysql_reader.read ( """
        SELECT id as question_id, question_txt, question_type, 
               SUBSTRING(question_txt, 1, 50) as name
        FROM test_question_info
    """ )
    neo4j_writer.write_nodes ( "试题" , "question_id" , questions )

    print( "课程数据节点导入完成" )

    # 建立课程数据关系
    # 1. 学科-分类关系
    subject_category_relations = [
        {"start_id" : sub["subject_id"] , "end_id" : sub["category_id"]}
        for sub in subjects if sub.get ( "category_id" )
    ]
    neo4j_writer.write_relations ( "BELONG" , "学科" , "分类" , "subject_id" , "category_id" ,
                                   subject_category_relations )

    # 2. 课程-学科关系
    course_subject_relations = [
        {"start_id" : course["course_id"] , "end_id" : course["subject_id"]}
        for course in courses if course.get ( "subject_id" )
    ]
    neo4j_writer.write_relations ( "BELONG" , "课程" , "学科" , "course_id" , "subject_id" , course_subject_relations )

    # 3. 章节-课程关系
    chapter_course_relations = [
        {"start_id" : chapter["chapter_id"] , "end_id" : chapter["course_id"]}
        for chapter in chapters if chapter.get ( "course_id" )
    ]
    neo4j_writer.write_relations ( "BELONG" , "章节" , "课程" , "chapter_id" , "course_id" , chapter_course_relations )

    # 4. 视频-章节关系
    video_chapter_relations = [
        {"start_id" : video["video_id"] , "end_id" : video["chapter_id"]}
        for video in videos if video.get ( "chapter_id" )
    ]
    neo4j_writer.write_relations ( "BELONG" , "视频" , "章节" , "video_id" , "chapter_id" , video_chapter_relations )

    # 5. 试卷-课程关系
    paper_course_relations = [
        {"start_id" : paper["paper_id"] , "end_id" : paper["course_id"]}
        for paper in papers if paper.get ( "course_id" )
    ]
    neo4j_writer.write_relations ( "BELONG" , "试卷" , "课程" , "paper_id" , "course_id" , paper_course_relations )

    # 6. 课程-教师关系
    course_teacher_relations = [
        {"start_id" : course["course_id"] , "end_id" : course["teacher"]}
        for course in courses if course.get ( "teacher" )
    ]
    neo4j_writer.write_relations ( "HAVE" , "课程" , "教师" , "course_id" , "teacher_name" , course_teacher_relations )

    # 7. 课程-价格关系
    course_price_relations = [
        {"start_id" : course["course_id"] , "end_id" : f"price_{course['course_id']}"}
        for course in courses
    ]
    neo4j_writer.write_relations ( "HAVE" , "课程" , "价格" , "course_id" , "price_id" , course_price_relations )

    print( "课程数据关系建立完成" )


def migrate_student_behavior ( mysql_reader , neo4j_writer ) :
    """迁移学生行为数据"""
    print( "开始迁移学生行为数据..." )

    # 1. 导入学生
    students = mysql_reader.read ( """
        SELECT id as uid, login_name, nick_name, real_name, phone_num, email, 
               birthday as 生日, gender as 性别
        FROM user_info
    """ )
    neo4j_writer.write_nodes ( "学生" , "uid" , students )

    # 2. 导入收藏关系
    favors = mysql_reader.read ( "SELECT user_id, course_id, create_time as 时间 FROM favor_info" )
    favor_relations = [
        {"start_id" : favor["user_id"] , "end_id" : favor["course_id"] , "时间" : favor["时间"]}
        for favor in favors
    ]
    neo4j_writer.write_relations (
        "FAVOR" , "学生" , "课程" , "uid" , "course_id" ,
        favor_relations , rel_properties=["时间"]
    )

    # 3. 导入答题关系
    exam_questions = mysql_reader.read ( """
        SELECT user_id, question_id, is_correct as 是否正确, score as 得分 
        FROM test_exam_question
    """ )

    answer_relations = [
        {"start_id" : eq["user_id"] , "end_id" : eq["question_id"] , "是否正确" : eq["是否正确"] , "得分" : float(eq["得分"])}
        for eq in exam_questions
    ]
    neo4j_writer.write_relations (
        "ANSWER" , "学生" , "试题" , "uid" , "question_id" ,
        answer_relations , rel_properties=["是否正确" , "得分"]
    )

    # 4. 导入观看关系
    # 首先需要建立视频和章节的映射
    video_chapter_map = {v["video_id"] : v["chapter_id"] for v in
                         mysql_reader.read ( "SELECT id as video_id, chapter_id FROM video_info" )}

    progresses = mysql_reader.read (
        "SELECT user_id, chapter_id, position_sec as 进度, update_time as 最后观看时间 FROM user_chapter_progress" )
    watch_relations = []

    for progress in progresses :
        # 找到该章节下的所有视频
        chapter_videos = mysql_reader.read (
            f"SELECT id as video_id FROM video_info WHERE chapter_id = {progress['chapter_id']}" )
        for video in chapter_videos :
            watch_relations.append ( {
                "start_id" : progress["user_id"] ,
                "end_id" : video["video_id"] ,
                "进度" : progress["进度"] ,
                "最后观看时间" : progress["最后观看时间"]
            } )

    neo4j_writer.write_relations (
        "WATCH" , "学生" , "视频" , "uid" , "video_id" ,
        watch_relations , rel_properties=["进度" , "最后观看时间"]
    )

    print( "学生行为数据迁移完成" )

def migrate_knowledge_points (mysql_reader , neo4j_writer) :
    """迁移知识点数据"""
    print( "=== 开始迁移知识点数据 ===" )

    # 1. 导入已有知识点
    knowledge_points = mysql_reader.read ( "SELECT id, point_txt as name FROM knowledge_point" )
    # knowledge_points = _validate_data ( knowledge_points , ['id' , 'name'] )
    neo4j_writer.write_nodes ( "知识点" , "id" , knowledge_points )

    # 2. 建立知识点与课程的关系
    courses = mysql_reader.read ( "SELECT id as course_id, course_name as name FROM course_info" )
    course_knowledge_relations = []

    for course in courses :
        # 创建基于课程名称的知识点
        knowledge_name = course["name"]
        course_knowledge_relations.append ( {
            "start_id" : course["course_id"] ,
            "end_id" : knowledge_name
        } )

    # 先写入这些知识点
    course_knowledges = [{"id" : course["name"] , "name" : course["name"]} for course in courses]
    neo4j_writer.write_nodes ( "知识点" , "id" , course_knowledges )

    # 建立课程-知识点关系
    neo4j_writer.write_relations ( "HAVE" , "课程" , "知识点" , "course_id" , "id" ,
                                        course_knowledge_relations )

    # 3. 建立知识点与章节的关系
    chapters = mysql_reader.read ( "SELECT id as chapter_id, chapter_name as name FROM chapter_info" )
    chapter_knowledge_relations = []

    for chapter in chapters :
        knowledge_name = chapter["name"]
        chapter_knowledge_relations.append ( {
            "start_id" : chapter["chapter_id"] ,
            "end_id" : knowledge_name
        } )

    # 写入章节知识点
    chapter_knowledges = [{"id" : chapter["name"] , "name" : chapter["name"]} for chapter in chapters]
    neo4j_writer.write_nodes ( "知识点" , "id" , chapter_knowledges )

    # 建立章节-知识点关系
    neo4j_writer.write_relations ( "HAVE" , "章节" , "知识点" , "chapter_id" , "id" ,
                                        chapter_knowledge_relations )

    # 4. 建立知识点与试题的关系
    questions = mysql_reader.read ( "SELECT id as question_id, question_txt FROM test_question_info" )
    question_knowledge_relations = []

    for question in questions :
        knowledge_name = question["question_txt"][:50] + "..." if len ( question["question_txt"] ) > 50 else \
        question["question_txt"]
        question_knowledge_relations.append ( {
            "start_id" : question["question_id"] ,
            "end_id" : knowledge_name
        } )

    # 写入试题知识点
    question_knowledges = [{"id" : q["question_txt"][:50] + "..." , "name" : q["question_txt"][:50] + "..."} for q
                           in questions]
    neo4j_writer.write_nodes ( "知识点" , "id" , question_knowledges )

    # 建立试题-知识点关系
    neo4j_writer.write_relations ( "HAVE" , "试题" , "知识点" , "question_id" , "id" ,
                                        question_knowledge_relations )

    print( "知识点数据迁移完成" )






# 主函数
def main () :
    """主函数，执行数据迁移"""
    start_time = time.time ( )

    # 初始化读写器
    mysql_reader = MysqlReader ( config.MYSQL_CONFIG )
    neo4j_writer = Neo4jWriter ( config.NEO4J_CONFIG )

    try :
        # 执行数据迁移
        migrate_course_data ( mysql_reader , neo4j_writer )
        migrate_student_behavior ( mysql_reader , neo4j_writer )
        migrate_knowledge_points ( mysql_reader , neo4j_writer )

        end_time = time.time ( )
        print( f"数据迁移完成，总耗时: {end_time - start_time:.2f} 秒" )

    except Exception as e :
        print( f"数据迁移过程中发生错误: {e}" )
    finally :
        # 关闭连接
        mysql_reader.close ( )
        neo4j_writer.close ( )



if __name__ == "__main__" :
    main ( )