from datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

        self.teacher_mapping = {}
        self.price_mapping = {}

    def sync_category(self):
        # 同步分类数据
        sql = """
            select  id,
            category_name as name
            from base_category_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("category", properties)

    def sync_subject(self):
        # 同步学科
        sql = """
            select  id,
            subject_name as name
            from base_subject_info 
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("subject", properties)

    def sync_course(self):
        # 同步课程
        sql = """
            select  id,
            course_name as name
            from course_info 
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("course", properties)

    def sync_teacher(self):
        # 同步教师（从course_info中提取唯一的teacher值）
        sql = """
            select distinct teacher as name
            from course_info 
            where teacher is not null and teacher != ''
        """
        teachers = self.mysql_reader.read(sql)

        # 为每个teacher生成顺序ID并建立映射
        properties = []
        for i, teacher in enumerate(teachers):
            teacher_id = f"teacher_{i}"
            properties.append({'id': teacher_id, 'name': teacher['name']})
            # 存储teacher名称到ID的映射
            self.teacher_mapping[teacher['name']] = teacher_id

        self.neo4j_writer.write_node("teacher", properties)
        return properties  # 返回teacher信息用于建立关系

    def sync_price(self):
        # 同步价格（从course_info中提取唯一的price值）
        sql = """
            select distinct actual_price as name
            from course_info 
            where actual_price is not null
        """
        prices = self.mysql_reader.read(sql)

        # 为每个price生成顺序ID并建立映射
        properties = []
        for i, price in enumerate(prices):
            price_id = f"price_{i}"
            price_value = str(price['name'])
            properties.append({'id': price_id, 'name': price_value})
            # 存储price值到ID的映射
            self.price_mapping[price_value] = price_id

        self.neo4j_writer.write_node("price", properties)
        return properties  # 返回price信息用于建立关系

    def sync_chapter(self):
        # 同步章节
        sql = """
            select  id,
            chapter_name as name
            from chapter_info 
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("chapter", properties)

    def sync_video(self):
        # 同步视频
        sql = """
            select  id,
            video_name as name
            from video_info 
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("video", properties)

    def sync_paper(self):
        # 试卷
        sql = """
            select  id,
            paper_title as name
            from test_paper
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("paper", properties)

    def sync_question(self):
        # 试题
        sql = """
            select  id,
            question_txt as name
            from test_question_info
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("question", properties)

    def sync_user(self):
        #学生
        sql = """
            select  id,
            real_name as name,
            birthday as birth,
            case when gender is null then "Unknown" else  gender end as gender
            from user_info 
        """
        properties = self.mysql_reader.read(sql)
        # 对于用户节点，需要自定义写入逻辑，因为属性不只有name
        cypher = """
            UNWIND $batch AS item
            MERGE (:user{id:item.id, name:item.name, birth:item.birth,gender:item.gender})
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, properties)

    def sync_knowledge(self):
        #知识点
        sql = """
            select  id,
            point_txt as name
            from knowledge_point
        """
        properties = self.mysql_reader.read(sql)
        self.neo4j_writer.write_node("knowledge", properties)

    def sync_subject_to_category(self):
        #学科和类别
        sql = """
            select id as start_id,
            category_id as end_id
            from base_subject_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("subject", "category", "BELONG", relations)

    def sync_course_to_subject(self):
        #课程和学科
        sql = """
            select id as start_id,
            subject_id as end_id
            from course_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("course", "subject", "BELONG", relations)

    def sync_chapter_to_course(self):
        #章节和课程
        sql = """
            select id as start_id,
            course_id as end_id
            from chapter_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("chapter", "course", "BELONG", relations)

    def sync_video_to_chapter(self):
        #视频和章节
        sql = """
            select id as start_id,
            chapter_id as end_id
            from video_info
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("video", "chapter", "BELONG", relations)

    def sync_paper_to_course(self):
        #试卷和课程
        sql = """
            select id as start_id,
            course_id as end_id
            from test_paper
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("paper", "course", "BELONG", relations)

    def sync_question_to_paper(self):
        #试题和试卷
        sql = """
            select id as start_id,
            paper_id as end_id
            from test_paper_question
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("question", "paper", "BELONG", relations)

    def sync_user_favorite(self):
        #用户收藏
        sql = """
            SELECT user_id as start_id, 
                   course_id as end_id, 
                   create_time as favor_time
            FROM favor_info
        """
        relations = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch AS item
            MATCH (start:user{id:item.start_id}), (end:course{id:item.end_id})
            MERGE (start)-[:FAVOR{favor_time:item.favor_time}]->(end)
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, relations)

    def sync_user_answer(self):
        #用户考试答案
        sql = """
            select user_id as start_id, 
            question_id as end_id, 
            create_time as answer_time,
            is_correct as answer
            from test_exam_question
        """
        relations = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch AS item
            MATCH (start:user{id:item.start_id}), (end:question{id:item.end_id})
            MERGE (start)-[:ANSWER{answer_time:item.answer_time, answer:item.answer}]->(end)
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, relations)

    def sync_user_chapter_progress(self):
        #观看进程
        sql = """
            select u.user_id as start_id, 
            v.id as end_id, 
            u.position_sec as watch_progress,
            u.create_time as last_watch_time
            from user_chapter_progress u
            join video_info v
            on u.course_id=v.course_id and u.chapter_id=v.chapter_id
        """
        relations = self.mysql_reader.read(sql)
        cypher = """
            UNWIND $batch AS item
            MATCH (start:user{id:item.start_id}), (end:video{id:item.end_id})
            MERGE (start)-[:WATCH{watch_progress:item.watch_progress, last_watch_time:item.last_watch_time}]->(end)
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, relations)

    def sync_course_have_teacher(self):
        #课程和教师
        sql = """
            select c.id as start_id,
                   c.teacher as teacher_name
            from course_info c
            where c.teacher is not null and c.teacher != ''
        """
        courses = self.mysql_reader.read(sql)

        relations = []
        for course in courses:
            teacher_name = course['teacher_name']
            if teacher_name in self.teacher_mapping:
                teacher_id = self.teacher_mapping[teacher_name]
                relations.append({
                    'start_id': course['start_id'],
                    'end_id': teacher_id,
                    'teacher_name': teacher_name
                })
        cypher = """
            UNWIND $batch AS item
            MATCH (start:course{id:item.start_id}), (end:teacher{id:item.end_id})
            MERGE (start)-[:HAVE{type: 'teacher'}]->(end)
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, relations)

    def sync_course_have_price(self):
        # 同步课程与价格
        sql = """
            select c.id as start_id,
                   c.actual_price as price_value
            from course_info c
            where c.actual_price is not null
        """
        courses = self.mysql_reader.read(sql)

        relations = []
        for course in courses:
            price_value = str(course['price_value'])
            if price_value in self.price_mapping:
                price_id = self.price_mapping[price_value]
                relations.append({
                    'start_id': course['start_id'],
                    'end_id': price_id,
                    'price_value': price_value
                })

        cypher = """
            UNWIND $batch AS item
            MATCH (start:course{id:item.start_id}), (end:price{id:item.end_id})
            MERGE (start)-[:HAVE{type: 'price'}]->(end)
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, relations)


    ##知识点关系

    #课程，知识点
    def sync_course_to_knowledge(self):
        sql = """
            select distinct ci.id as start_id,
                   kp.id as end_id
            from course_info ci
            join knowledge_point kp on ci.course_introduce like concat('%', kp.point_txt, '%')
            where ci.deleted = '0' and kp.deleted = '0'
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("course", "knowledge", "HAVE", relations)

    #章节，知识点
    def sync_chapter_to_knowledge(self):
        sql = """
            select distinct chi.id as start_id,
                   kp.id as end_id
            from chapter_info chi
            join knowledge_point kp on chi.chapter_name like concat('%', kp.point_txt, '%')
            where chi.deleted = '0' and kp.deleted = '0'
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("chapter", "knowledge", "HAVE", relations)

    # 从试题内容中抽取的知识点
    def sync_question_to_knowledge(self):

        sql = """
            select distinct tqi.id as start_id,
                   kp.id as end_id
            from test_question_info tqi
            join knowledge_point kp on tqi.question_txt like concat('%', kp.point_txt, '%')
            where tqi.deleted = '0' and kp.deleted = '0'
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("question", "knowledge", "HAVE", relations)

    # 试题与知识点的关系
    def sync_explicit_question_to_knowledge(self):

        sql = """
            select tpq.question_id as start_id,
                   tpq.point_id as end_id
            from test_point_question tpq
            join knowledge_point kp on tpq.point_id = kp.id
            join test_question_info tqi on tpq.question_id = tqi.id
            where tpq.deleted = '0' and kp.deleted = '0' and tqi.deleted = '0'
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("question", "knowledge", "HAVE", relations)

    def sync_knowledge_need_relation(self):
        # 知识点之间的先修关系（NEED）
        #章节顺序知识点的先修
        sql = """
            select kp1.knowledge_id as start_id,
                   kp2.knowledge_id as end_id
            from (
                select chi.course_id, chi.id as chapter_id, kp.id as knowledge_id,
                       row_number() over (partition by chi.course_id order by chi.id) as chapter_order
                from chapter_info chi
                join knowledge_point kp on chi.chapter_name like concat('%', kp.point_txt, '%')
                where chi.deleted = '0' and kp.deleted = '0'
            ) kp1
            join (
                select chi.course_id, chi.id as chapter_id, kp.id as knowledge_id,
                       row_number() over (partition by chi.course_id order by chi.id) as chapter_order
                from chapter_info chi
                join knowledge_point kp on chi.chapter_name like concat('%', kp.point_txt, '%')
                where chi.deleted = '0' and kp.deleted = '0'
            ) kp2 on kp1.course_id = kp2.course_id and kp1.chapter_order = kp2.chapter_order - 1
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("knowledge", "knowledge", "NEED", relations)

    def sync_knowledge_belong_relation(self):
        # 知识点之间的包含关系（BELONG）
        # 课程-章节
        sql = """
            select kp_course.id as start_id,
                   kp_chapter.id as end_id
            from (
                select ci.id as course_id, kp.id, kp.point_txt
                from course_info ci
                join knowledge_point kp on ci.course_introduce like concat('%', kp.point_txt, '%')
                where ci.deleted = '0' and kp.deleted = '0'
            ) kp_course
            join (
                select chi.course_id, kp.id, kp.point_txt
                from chapter_info chi
                join knowledge_point kp on chi.chapter_name like concat('%', kp.point_txt, '%')
                where chi.deleted = '0' and kp.deleted = '0'
            ) kp_chapter on kp_course.course_id = kp_chapter.course_id
            and kp_chapter.point_txt like concat('%', kp_course.point_txt, '%')
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("knowledge", "knowledge", "BELONG", relations)

    def sync_knowledge_related_relation(self):
        # 知识点之间的相关关系（RELATED）
        # 同一试卷下的试题知识点
        sql = """
            select distinct kp1.id as start_id,
                   kp2.id as end_id
            from test_paper_question tpq1
            join test_paper_question tpq2 on tpq1.paper_id = tpq2.paper_id and tpq1.question_id != tpq2.question_id
            join test_point_question tptq1 on tpq1.question_id = tptq1.question_id
            join test_point_question tptq2 on tpq2.question_id = tptq2.question_id
            join knowledge_point kp1 on tptq1.point_id = kp1.id
            join knowledge_point kp2 on tptq2.point_id = kp2.id
            where tpq1.deleted = '0' and tpq2.deleted = '0'
            and tptq1.deleted = '0' and tptq2.deleted = '0'
            and kp1.deleted = '0' and kp2.deleted = '0'
            and kp1.id != kp2.id
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("knowledge", "knowledge", "RELATED", relations)

    def sync_video_to_knowledge(self):
        # 视频与知识点的关系
        sql = """
            select distinct vi.id as start_id,
                   kp.id as end_id
            from video_info vi
            join knowledge_point kp on vi.video_name like concat('%', kp.point_txt, '%')
            where vi.deleted = '0' and kp.deleted = '0'
        """
        relations = self.mysql_reader.read(sql)
        self.neo4j_writer.write_relations("video", "knowledge", "HAVE", relations)

if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    synchronizer.sync_category()
    synchronizer.sync_subject()
    synchronizer.sync_course()
    synchronizer.sync_teacher()
    synchronizer.sync_price()
    synchronizer.sync_chapter()
    synchronizer.sync_video()
    synchronizer.sync_paper()
    synchronizer.sync_question()
    synchronizer.sync_user()
    synchronizer.sync_knowledge()

    # 同步关系
    synchronizer.sync_subject_to_category()
    synchronizer.sync_course_to_subject()
    synchronizer.sync_chapter_to_course()
    synchronizer.sync_paper_to_course()
    synchronizer.sync_video_to_chapter()
    synchronizer.sync_question_to_paper()
    synchronizer.sync_user_favorite()
    synchronizer.sync_user_answer()
    synchronizer.sync_user_chapter_progress()
    synchronizer.sync_course_have_teacher()
    synchronizer.sync_course_have_price()

    #知识点
    synchronizer.sync_course_to_knowledge()
    synchronizer.sync_chapter_to_knowledge()
    synchronizer.sync_question_to_knowledge()
    synchronizer.sync_explicit_question_to_knowledge()
    synchronizer.sync_video_to_knowledge()
    synchronizer.sync_knowledge_need_relation()
    synchronizer.sync_knowledge_belong_relation()
    synchronizer.sync_knowledge_related_relation()