import torch
from tenacity import sleep_using_event
from tqdm import tqdm
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config
from datasync.utils_neo4j_mysql import MysqlReader, Neo4jWriter
from uie_pytorch.uie_predictor import UIEPredictor


class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    @staticmethod
    def _init_extractor(list_input):
        # print(list_input[0:10])
        schema = ['TAG']
        # 设定抽取目标和定制化模型权重路径
        my_ie = UIEPredictor(model='uie-base',
                             task_path='D:\\PythonProject1\\PythonProject\\uie_pytorch\\check_point_course\\model_best',
                             schema=schema)
        list_tag = []
        for i in tqdm(range(0,len(list_input),50)):
            list_of_dict=my_ie(list_input[i:i+50])
            for dict_item in list_of_dict:
                list_item=[]
                if dict_item:
                    for item in dict_item['TAG']:
                        list_item.append(item['text'])
                list_tag.append(list_item)

        # print(list_tag[0:10])
        return list_tag

    def sync_tag_course(self):
        sql = """
              select id,
                     course_introduce
              from course_info
              """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        descs = [item['course_introduce'] for item in course_desc]
        # print(descs[0:10])
        tags_list = self._init_extractor(descs)

        tag_properties = []
        relations = []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                tag_id = '-'.join([str(id), str(index)])
                property = {'id': tag_id, 'name': tag}
                tag_properties.append(property)
                relation = {'start_id': id, 'end_id': tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes('Tag', tag_properties)
        self.neo4j_writer.write_relations('Have', 'course_info', 'Tag', relations)

    def sync_tag_chapter(self):
        sql = """
              select id,
                     text
              from chapter_info
              """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        descs = [item['text'] for item in course_desc]
        # print(descs[0:10])
        tags_list = self._init_extractor(descs)

        tag_properties = []
        relations = []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                tag_id = '-'.join([str(id), str(index)])
                property = {'id': tag_id, 'name': tag}
                tag_properties.append(property)
                relation = {'start_id': id, 'end_id': tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes('Tag', tag_properties)
        self.neo4j_writer.write_relations('Have', 'chapter_info', 'Tag', relations)

    def sync_tag_video(self):
        sql = """
              select id,
                     video_name
              from video_info
              """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        descs = [item['video_name'] for item in course_desc]
        # print(descs[0:10])
        tags_list = self._init_extractor(descs)

        tag_properties = []
        relations = []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                if tag:
                    tag_id = '-'.join([str(id), str(index)])
                    property = {'id': tag_id, 'name': tag}
                    tag_properties.append(property)
                    relation = {'start_id': id, 'end_id': tag_id}
                    relations.append(relation)
        self.neo4j_writer.write_nodes('Tag_video', tag_properties)
        self.neo4j_writer.write_relations('Have', 'video_info', 'Tag_video', relations)

    def sync_tag_question(self):
        sql = """
              select id,
                     question_txt
              from test_question_info
              """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        descs = [item['question_txt'] for item in course_desc]

        #进行数据清洗
        # for item in descs:
        #     item=item[3:-5]


        # print(descs[0:10])
        tags_list = self._init_extractor(descs)

        tag_properties = []
        relations = []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                if tag:
                    tag_id = '-'.join([str(id), str(index)])
                    property = {'id': tag_id, 'name': tag}
                    tag_properties.append(property)
                    relation = {'start_id': id, 'end_id': tag_id}
                    relations.append(relation)
        self.neo4j_writer.write_nodes('Tag_question', tag_properties)
        self.neo4j_writer.write_relations('Have', 'test_question_info', 'Tag_question', relations)
if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    # synchronizer.sync_tag_course()
    # synchronizer.sync_tag_chapter()
    # synchronizer.sync_tag_video()