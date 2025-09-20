from config.config import NEO4J_CONFIG
from data_sync.utils import MySqlReader, Neo4jWriter


class TableSync:
    def __init__(self):
        self.sql_reader=MySqlReader()
        self.neo4j_writer=Neo4jWriter()


    def sync_category(self):
        sql="""
        select id,
        category_name as name
        from base_category_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Category",res)

    def sync_subject(self):
        sql="""
        select id,
        subject_name as name
        from base_subject_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Subject",res)

    def sync_course(self):
        sql="""
        select id,
        course_name as name
        from course_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Course",res)

    def sync_teacher(self):
        sql="""
        SELECT 
        DISTINCT teacher AS name 
        FROM course_info;
        """
        res=self.sql_reader.read(sql)
        for index,dict_info in enumerate(res):
            dict_info["id"]=index
        self.neo4j_writer.writeNode("Teacher",res)

    def sync_price(self):
        sql = """
           SELECT 
           DISTINCT CAST(origin_price AS FLOAT) AS name
           FROM course_info;
           """
        res = self.sql_reader.read(sql)
        for index, dict_info in enumerate(res):
            dict_info["id"] = index
        self.neo4j_writer.writeNode("Price", res)

    def sync_chapter(self):
        sql="""
        select id,
        chapter_name as name
        from chapter_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Chapter",res)

    def sync_video(self):
        sql="""
        select id,
        video_name as name
        from video_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Video",res)

    def sync_paper(self):
        sql="""
        select id,
        paper_title as name
        from test_paper
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Paper",res)

    def sync_question(self):
        sql="""
        select id,
        question_txt as name
        from test_question_info
        """
        res=self.sql_reader.read(sql)
        self.neo4j_writer.writeNode("Question",res)

    def sync_course_to_subject(self):
        sql="""
        select id as start_id,
        subject_id as end_id
        from course_info
        """
        res=self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Course","Subject","BELONG",res)

    def sync_subject_to_category(self):
        sql="""
        select id as start_id,
        category_id as end_id
        from base_subject_info
        """
        res=self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Subject","Category","BELONG",res)

    def sync_chapter_to_course(self):
        sql="""
        select id as start_id,
        course_id as end_id
        from chapter_info
        """
        res=self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Chapter","Course","BELONG",res)

    def sync_video_to_chapter(self):
        sql = """
               select id as start_id,
               chapter_id as end_id
               from video_info
               """
        res = self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Video", "Chapter", "BELONG", res)

    def sync_paper_to_course(self):
        sql = """
               select id as start_id,
               course_id as end_id
               from test_paper
               """
        res = self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Paper", "Course", "BELONG", res)

    def sync_question_to_paper(self):
        sql = """
               select question_id as start_id,
               paper_id as end_id
               from test_paper_question
               """
        res = self.sql_reader.read(sql)
        self.neo4j_writer.writeRelation("Question", "Paper", "BELONG", res)

    def sync_course_to_teacher(self):
        sql = """
                       select id as start_id,
                       teacher as name
                       from course_info
                       """
        res = self.sql_reader.read(sql)
        teacher_name=[]
        for dict_info in res:
            teacher_name.append({"name":dict_info["name"]})
        cypher="""
        UNWIND $batch as item
        MATCH (n:Teacher{name:item.name})
        RETURN n.id as id
        """
        ans=self.neo4j_writer.read(cypher,batch=teacher_name)
        teacher_id = [record["id"] for record in ans.records]
        for index,dict_info in enumerate(res):
            dict_info["end_id"]=teacher_id[index]

        self.neo4j_writer.writeRelation("Course", "Teacher", "HAVE", res)

    def sync_course_to_price(self):
        sql = """
                       select id as start_id,
                       CAST(origin_price as FLOAT) as price
                       from course_info
                       """
        res = self.sql_reader.read(sql)
        teacher_name=[]
        for dict_info in res:
            teacher_name.append({"price":dict_info["price"]})
        cypher="""
        UNWIND $batch as item
        MATCH (n:Price{name:item.price})
        RETURN n.id as id
        """
        ans=self.neo4j_writer.read(cypher,batch=teacher_name)
        teacher_id = [record["id"] for record in ans.records]
        for index,dict_info in enumerate(res):
            dict_info["end_id"]=teacher_id[index]

        self.neo4j_writer.writeRelation("Course", "Price", "HAVE", res)

    def sync_student(self):
        sql="""
        select id as uid,
        birthday,
        case when gender is null then 'Unknown' else gender end as gender
        from user_info
        """
        res=self.sql_reader.read(sql)
        print(res)
        cypher="""
        UNWIND $batch as item
        MERGE (:Student {uid:item.uid,birthday:toString(item.birthday),gender:item.gender})
        """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher,res)

    def sync_student_to_course(self):
        sql="""
        select user_id as start_id,
        course_id as end_id,
        create_time as create_time
        from favor_info
        """
        res=self.sql_reader.read(sql)

        cypher = """
                    UNWIND $batch AS item
                    MATCH (start:Student{uid:item.start_id}), (end:Course{id:item.end_id})
                    MERGE (start)-[:FAVOR {create_time:toString(item.create_time)}]->(end)
                """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher,res)

    def sync_student_to_question(self):
        sql="""
        select user_id as start_id,
        question_id as end_id,
        is_correct as is_correct
        from test_exam_question
        """
        res=self.sql_reader.read(sql)

        cypher = """
                UNWIND $batch AS item
                MATCH (start:Student{uid:item.start_id}), (end:Question{id:item.end_id})
                MERGE (start)-[:ANSWER {is_correct:item.is_correct}]->(end)
                """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, res)

    def sync_student_to_video(self):
        sql="""
        select u.user_id as start_id,
        v.id as end_id,
        u.position_sec as progress,
        u.create_time as final_watch_time 
        from user_chapter_progress u 
        join video_info v 
        on u.course_id=v.course_id and u.chapter_id=v.chapter_id
        """
        res = self.sql_reader.read(sql)
        print(res)
        cypher = """
                    UNWIND $batch AS item
                    MATCH (start:Student{uid:item.start_id}), (end:Video{id:item.end_id})
                    MERGE (start)-[:WATCH {progress:item.progress,final_watch_time:toString(item.final_watch_time)}]->(end)
                """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher, res)

    # def sync_knowledge(self):
    #     sql="""
    #     select id,
    #     point_txt as name
    #     from knowledge_point
    #     """
    #     res=self.sql_reader.read(sql)
    #     self.neo4j_writer.writeNode("KnowledgePoint",res)
    #
    # def sync_question_to_knowledge(self):
    #     sql="""
    #     select question_id as start_id,
    #     point_id as end_id
    #     from test_point_question
    #     """
    #     res=self.sql_reader.read(sql)
    #     self.neo4j_writer.writeRelation("Question","KnowledgePoint","HAVE",res)
    #
    # def sync_course_to_knowledge(self):
    #     sql="""
    #     select distinct
    #     tp.course_id as start_id,
    #     tq.point_id as end_id
    #     from test_point_question tq
    #     join test_paper_question tpq on tq.question_id = tpq.question_id
    #     join test_paper tp on tpq.paper_id = tp.id
    #     """
    #     res=self.sql_reader.read(sql)
    #     self.neo4j_writer.writeRelation("Course","KnowledgePoint","HAVE",res)


if __name__=="__main__":
    table_sync=TableSync()
    # table_sync.sync_category()
    # table_sync.sync_subject()
    # table_sync.sync_course()
    # table_sync.sync_teacher()
    # table_sync.sync_price()
    # table_sync.sync_chapter()
    # table_sync.sync_video()
    # table_sync.sync_paper()
    # table_sync.sync_question()

    # table_sync.sync_course_to_subject()
    # table_sync.sync_subject_to_category()
    # table_sync.sync_chapter_to_course()
    # table_sync.sync_video_to_chapter()
    # table_sync.sync_paper_to_course()
    # table_sync.sync_question_to_paper()
    # table_sync.sync_course_to_teacher()
    # table_sync.sync_course_to_price()

    table_sync.sync_student()
    # table_sync.sync_student_to_course()
    table_sync.sync_student_to_question()
    # table_sync.sync_student_to_video()

    # table_sync.sync_knowledge()
    # table_sync.sync_question_to_knowledge()
    # table_sync.sync_course_to_knowledge()