import torch
from tenacity import sleep_using_event
from transformers import AutoModelForTokenClassification, AutoTokenizer
import sys

print(sys.path)
from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter
from models.uie_pytorch.uie_predictor import UIEPredictor


class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extractor()

    def _init_extractor(self):
        uie = UIEPredictor(model='uie-base', task_path=str(
            config.ROOT_DIR / 'src' / 'models' / 'uie_pytorch' / 'checkpoint' / 'model_best'))
        return uie

    def sync_tag(self):
        # 课程介绍
        print('课程介绍')
        sql = """
              select id,
                     course_introduce
              from course_info
              """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        course_introduce = [item['course_introduce'] for item in course_desc]
        print(course_introduce)
        tags_list = self.extractor(course_introduce)
        print(tags_list)
        self.create_node_and_relation('course_info', ids, tags_list)

        # 章节介绍
        print('章节介绍')
        sql = """
              select id,
                     chapter_name
              from chapter_info
              """
        chapter_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in chapter_desc]
        chapter_introduce = [item['chapter_name'] for item in chapter_desc]
        tags_list = self.extractor(chapter_introduce)
        self.create_node_and_relation('chapter_info', ids, tags_list)

        # 问题描述
        print('问题描述')
        sql = """
              select id,
                     question_txt
              from test_question_info
              """
        question_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in question_desc]
        question_introduce = [item['question_txt'] for item in question_desc]
        tags_list = self.extractor(question_introduce)
        self.create_node_and_relation('test_question_info', ids, tags_list)

    def create_node_and_relation(self, start_label, ids, tags_list):
        tag_properties = []
        relations = []
        for id, tags in zip(ids, tags_list):
            tags = tags.get('知识点')
            if tags is None:
                continue
            for index, tag in enumerate(tags):
                tag_id = '-'.join([str(id), str(index)])
                property = {'id': tag_id, 'name': tag['text']}
                tag_properties.append(property)
                relation = {'start_id': id, 'end_id': tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes('Tag', tag_properties)
        self.neo4j_writer.write_relations('Have', start_label, 'Tag', relations)


if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    synchronizer.sync_tag()
