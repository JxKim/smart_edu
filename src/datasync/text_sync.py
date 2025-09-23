import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config
from datasync.read_write_utils import MysqlReader, Neo4jWriter
from models.ner.predict import Predictor


class TextSync:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extractor()

    @staticmethod
    def _init_extractor():
        model = AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')
        tokenizer = AutoTokenizer.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        return Predictor(model,tokenizer,device)

    #🍅同步非结构化数据
    def sync_course_tag(self):
        sql = '''
            select id,course_introduce
            from course_info
        '''
        course_desc = self.mysql_reader.read(sql)
        #[{'id': 39, 'course_introduce': '本课程为Java...'}, {'id': 42, 'course_introduce': '讲解Maven项...'}，...]
        # print(course_desc)
        ids = [item['id'] for item in course_desc]
        descs = [item['course_introduce'] for item in course_desc]
        tags_list = self.extractor.extract(descs)

        tag_properties = []
        relations = []
        for id,tags in zip(ids,tags_list):
            for index,tag in enumerate(tags):
                tag_id = '_'.join([str(id),str(index)])
                property = {'id':tag_id,'name':tag}
                tag_properties.append(property)
                relation = {'start_id':id,'end_id':tag_id}
                relations.append(relation)

        self.neo4j_writer.write_nodes('Course_Tag',tag_properties)
        self.neo4j_writer.write_relations('Have','Course','Course_Tag',relations)
        print('课程-[:HAVE]-->知识点')

    #🍅同步非结构化数据  使用批处理抽取 不进行批处理会有oom问题❌
    def sync_chapter_tag(self):
        sql = '''
            select id,chapter_name
            from chapter_info
        '''
        chapter_desc = self.mysql_reader.read(sql)
        #[{'id': 39, 'course_introduce': '本课程为Java...'}, {'id': 42, 'course_introduce': '讲解Maven项...'}，...]
        # print(course_desc)
        ids = [item['id'] for item in chapter_desc]
        descs = [item['chapter_name'] for item in chapter_desc]

        #🔥拆分descs为小批次
        batch_size = 100
        tags_list = []
        for i in range(0,len(descs),batch_size):
            batch_descs = descs[i:i+batch_size]
            batch_tags = self.extractor.extract(batch_descs)
            tags_list.extend(batch_tags)

        tag_properties = []
        relations = []
        for id,tags in zip(ids,tags_list):
            for index,tag in enumerate(tags):
                tag_id = '_'.join([str(id),str(index)])
                property = {'id':tag_id,'name':tag}
                tag_properties.append(property)
                relation = {'start_id':id,'end_id':tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes('Chapter_Tag',tag_properties)
        self.neo4j_writer.write_relations('Have','Chapter','Chapter_Tag',relations)
        print('章节-[:HAVE]-->知识点')

    #🍅同步非结构化数据 使用批处理抽取 不进行批处理会有oom问题❌
    def sync_test_question_tag(self):
        sql = '''
            select id, question_txt
            from test_question_info
        '''
        test_question_desc = self.mysql_reader.read(sql)
        #[{'id': 39, 'course_introduce': '本课程为Java...'}, {'id': 42, 'course_introduce': '讲解Maven项...'}，...]
        # print(course_desc)
        ids = [item['id'] for item in test_question_desc]
        descs = [item['question_txt'] for item in test_question_desc]

        # 🔥拆分descs为小批次
        batch_size = 100
        tags_list = []
        for i in range(0, len(descs), batch_size):
            batch_descs = descs[i:i + batch_size]
            batch_tags = self.extractor.extract(batch_descs)
            tags_list.extend(batch_tags)

        tag_properties = []
        relations = []
        for id,tags in zip(ids,tags_list):
            for index,tag in enumerate(tags):
                tag_id = '_'.join([str(id),str(index)])
                property = {'id':tag_id,'name':tag}
                tag_properties.append(property)
                relation = {'start_id':id,'end_id':tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes('TestQuestion_Tag',tag_properties)
        self.neo4j_writer.write_relations('Have','TestQuestion','TestQuestion_Tag',relations)
        print('试题-[:HAVE]->知识点')

if __name__ == '__main__':
    text_sync = TextSync()

    text_sync.sync_course_tag()
    text_sync.sync_chapter_tag()
    text_sync.sync_test_question_tag()



























