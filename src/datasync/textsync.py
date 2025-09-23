from configuration.config import ROOT_DIR
from datasync.utils import Neo4jUtil, MysqlUtil
from uie_pytorch.uie_predictor import UIEPredictor

predictor = UIEPredictor(model='uie-base', schema=['TAG'], engine='pytorch', device='gpu', batch_size=16, task_path=ROOT_DIR /'src'/'uie_pytorch'/'checkpoint'/'model_best')

class TextSync:
    def __init__(self):
        self.predictor = predictor
        self.graph = Neo4jUtil()
        self.sql_query = MysqlUtil()
    def sync_course(self):
        sql = """
            select course_introduce, 
                                id
            from course_info
        """

        course_introduce = [description['course_introduce'] for description in self.sql_query.read(sql)]

        course_id = [description['id'] for description in self.sql_query.read(sql)]
        course_knowledge_points_lists = [predict["TAG"] for predict in self.predictor.predict(course_introduce)]

        course_knowledge_points = []
        for knowledge_points_list in course_knowledge_points_lists:
            course_knowledge_points.append([knowledge_point['text'] for knowledge_point in knowledge_points_list])
        point_property = []
        relations = []
        for idx, points_list in zip(course_id ,course_knowledge_points):
            for index, point in enumerate(points_list):
                point_id = '-'.join([str(idx), str(index)])
                property = {"id": point_id, "name": point}
                point_property.append(property)
                relations.append({'start_id':idx, 'end_id': point_id})
        self.graph.writer_nodes('course_knowledge_point', point_property)
        self.graph.writer_relations("Have", "Course", "course_knowledge_point", relations)
    def sync_chapter(self):
        sql = """
            select chapter_name, 
                            id
            from ai_edu.chapter_info
        """

        chapter_name = [description['chapter_name'] for description in self.sql_query.read(sql)]

        chapter_id = [description['id'] for description in self.sql_query.read(sql)]
        chapter_predict = self.predictor.predict(chapter_name)

        filtered = [
            (name, id_, predict)
            for name, id_, predict in zip(chapter_name, chapter_id, chapter_predict)
            if "TAG" in predict
        ]

        chapter_name = [x[0] for x in filtered]
        chapter_id = [x[1] for x in filtered]
        chapter_predict = [x[2] for x in filtered]

        chapter_knowledge_points_lists = [predict["TAG"] for predict in chapter_predict]

        chapter_knowledge_points = []
        for knowledge_points_list in chapter_knowledge_points_lists:
            chapter_knowledge_points.append([knowledge_point['text'] for knowledge_point in knowledge_points_list])
        point_property = []
        relations = []
        for idx, points_list in zip(chapter_id ,chapter_knowledge_points):
            for index, point in enumerate(points_list):
                point_id = '-'.join([str(idx), str(index)])
                property = {"id": point_id, "name": point}
                point_property.append(property)
                relations.append({'start_id':idx, 'end_id': point_id})
        self.graph.writer_nodes('chapter_knowledge_point', point_property)
        self.graph.writer_relations("Have", "Chapter", "chapter_knowledge_point", relations)
    def sync_video(self):
        sql = """
            select video_name, 
                            id
            from video_info
        """

        video_name = [description['video_name'] for description in self.sql_query.read(sql)]

        video_id = [description['id'] for description in self.sql_query.read(sql)]
        video_predict = self.predictor.predict(video_name)
        filtered = [
            (name, id_, predict)
            for name, id_, predict in zip(video_name, video_id, video_predict)
            if "TAG" in predict
        ]

        video_name = [x[0] for x in filtered]
        video_id = [x[1] for x in filtered]
        video_predict = [x[2] for x in filtered]
        video_knowledge_points_lists = [predict["TAG"] for predict in video_predict]

        video_knowledge_points = []
        for knowledge_points_list in video_knowledge_points_lists:
            video_knowledge_points.append([knowledge_point['text'] for knowledge_point in knowledge_points_list])
        point_property = []
        relations = []
        for idx, points_list in zip(video_id ,video_knowledge_points):
            for index, point in enumerate(points_list):
                point_id = '-'.join([str(idx), str(index)])
                property = {"id": point_id, "name": point}
                point_property.append(property)
                relations.append({'start_id':idx, 'end_id': point_id})
        self.graph.writer_nodes('video_knowledge_point', point_property)
        self.graph.writer_relations("Have", "Video", "video_knowledge_point", relations)

if __name__ == "__main__":
    text_sync = TextSync()
    # text_sync.sync_course()
    text_sync.sync_chapter()
    text_sync.sync_video()