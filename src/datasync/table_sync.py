from datasync.utils import MysqlReader, Neo4jWriter

class TableSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    #　创建视频节点
    def sync_video(self):
        sql ="""
            select 
                id,
                video_name as name
            from
                video_info
            """
        # 执行sql语句
        propetries = self.mysql_reader.read(sql)
        # 创建节点
        self.neo4j_writer.write_nodes("video", propetries)

    def sync_video_to_cheapter(self):
        sql = """
                select
                    id as start_id,
                    chapter_id as end_id
                from
                    video_info
        """
        # 执行sql语句
        propetries = self.mysql_reader.read(sql)
        # 创建节点
        # video属于chapter
        self.neo4j_writer.write_relations("belong", start_label="video", \
                                          end_label="chapter", relations=propetries)

    # 创建试题节点
    def sync_question(self):
        sql = """
                select
                    id,
                    question_txt as name
                from
                    test_question_info
        """
        properties = self.mysql_reader.read(sql)
        # 创建节点
        cypher = """
                    UNWIND $batch AS item
                    MERGE (n:question {id:item.id, name:item.name} )
                    """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

    # 创建试题－【ｂｅｌｏｎｇ】》试卷
    def sync_question_to_paper(self):
        sql = """
                select
                    question_id as start_id,
                    paper_id as end_id
                from
                    test_paper_question
        """
        self.neo4j_writer.write_relations("belong", start_label="question", \
                                          end_label="paper", relations=self.mysql_reader.read(sql))

    # 创建教师节点
    def sync_teacher(self):
        sql = """
                select distinct 
                    teacher
                from
                    course_info        
            """
        properties = self.mysql_reader.read(sql)
        cypher = """
                    UNWIND $batch AS item
                    MERGE (n:teacher { name:item.teacher } )
                    """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

    # 创建教师课程关系
    def sync_teacher_to_course(self):
        sql = """
                select
                    teacher
                    id
                from
                    course_info
        """
        propetries = self.mysql_reader.read(sql)
        cypher = """
                    unwind $batch as item
                    match ( start:course { id:item.id } ), ( end:teacher { name:item.teacher } )
                    merge (start)-[:have]->(end)    
        """
        self.neo4j_writer.driver.execute_query(cypher, batch=propetries)

    #　创建学生节点，属性有id, 生日，性别
    def sync_student(self):
        sql = """
                select
                    id,
                    birthday,
                    real_name as name,
                    gender
                from
                    user_info
        """
        properties = self.mysql_reader.read(sql)
        # gender为空的，转为unknown
        for item in properties:
            if item['gender'] == None:
                item['gender'] = 'unknown'
        cypher = """
                    UNWIND $batch AS item
                    MERGE (n:student {id:item.id, name:item.name, birthday:item.birthday, gender:item.gender} )
                    """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

    # 创建学生课程关系, 学生favor课程, 关系有时间属性
    def sync_student_to_course(self):
        sql = """
                select
                    user_id as start_id,
                    course_id as end_id,
                    create_time as time
                from
                    favor_info
        """
        properties = self.mysql_reader.read(sql)
        cypher = """
                    unwind $batch as item
                    match ( start:student { id:item.start_id } ), ( end:course { id:item.end_id } )
                    merge (start)-[r:favor {time:item.time}]->(end)    
        """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

    # 创建学生-试题关系，标签为answer, 属性为是否正确
    def sync_student_to_question(self):
        sql = """
                select
                    user_id as start_id,
                    question_id as end_id,
                    is_correct
                from
                    test_exam_question
        """
        properties = self.mysql_reader.read(sql)
        for item in properties:
            if item['is_correct'] == "1":
                item['is_correct'] = True
            elif item['is_correct'] == "0":
                item['is_correct'] = False
        cypher = """
                    unwind $batch as item
                    match ( start:student { id:item.start_id } ), ( end:question { id:item.end_id } )
                    merge (start)-[r:answer { is_correct:item.is_correct }]->(end)    
        """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

    # 创建学生-章节视频关系，标签为watch，属性为进度，最后观看时间
    def sync_student_to_video(self):
        sql = """
                select
                    user_id as start_id,
                    chapter_id as end_id,
                    position_sec as progress,
                    create_time as last_watch_time
                from
                    user_chapter_progress
        """
        properties = self.mysql_reader.read(sql)
        cypher = """
                    unwind $batch as item
                    match ( start:student { id:item.start_id } ), ( end:video { id:item.end_id } )
                    merge (start)-[r:watch {progress:item.progress, create_time:item.last_watch_time}]->(end)    
        """
        self.neo4j_writer.driver.execute_query(cypher, batch=properties)

if __name__ == '__main__':

    synchronizer = TableSynchronizer()

    # 查询课程教师
    # 建立视频节点
    # synchronizer.sync_video()
    #
    # # 视频和章节之间的关系，多个视频属于同一个章节
    # synchronizer.sync_video_to_cheapter()
    #
    # # 试题属于试卷
    # # 创建试题节点
    # synchronizer.sync_question()
    # synchronizer.sync_question_to_paper()
    #
    # # 课程 －【ｈａｖｅ】> 教师
    # synchronizer.sync_teacher()
    # synchronizer.sync_teacher_to_course()
    #
    # synchronizer.sync_student()
    # synchronizer.sync_student_to_course()
    synchronizer.sync_student_to_question()
    # synchronizer.sync_student_to_video()