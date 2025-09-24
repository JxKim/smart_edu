import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer
from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter
from models.ner.predict import Predictor

class TextSynchronizer:
    def __init__(self):
        #　封装了数据库连接和查询操作，提供统一的数据读取接口
        self.mysql_reader = MysqlReader()
        # 封装了图数据库的连接，和 节点、关系创建操作
        self.neo4j_writer = Neo4jWriter()
        # 模型的初始化和使用分离开来，初始化只用一次就行
        self.extractor = self._init_extractor()

    def _init_extractor(self):
        model = AutoModelForTokenClassification.from_pretrained( str(config.CHECKPOINT_DIR / 'ner' / 'best_model') )
        tokenizer = AutoTokenizer.from_pretrained( str(config.CHECKPOINT_DIR / 'ner' / 'best_model') )
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        return Predictor(model, tokenizer, device)

    def sync_chapter(self):
        sql = """
              select id,
                     chapter_name
              from chapter_info
              """
        # 从spu_info表中查询所有商品的ｉｄ和描述信息
        spu_desc = self.mysql_reader.read( sql )
        # 获取商品id 和 描述信息
        ids = [ item['id'] for item in spu_desc ]
        descs = [ item['chapter_name'] for item in spu_desc ]
        # 将descs输入model, 得到标签
        tags_list = self.extractor.extract(descs)
        # tags_list: [ [tag1, tag2], [tag1, tag2], [tag1, tag2], [tag1, tag2] ]

        tag_properties = []
        relations = []
        # 遍历商品id和标签
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                tag_id = '-'.join([str(id), str(index)])
                property = {'id': tag_id, 'name': tag}
                tag_properties.append(property)
                relation = {'start_id': id, 'end_id': tag_id}
                relations.append(relation)
        # 创建knowledge节点，tag_id为1-0, 1-1, 1-2
        self.neo4j_writer.write_nodes('knowledge', tag_properties)
        # 创建关系，SPU-Tag
        self.neo4j_writer.write_relations('Have', 'chapter', 'knowledge', relations)

    def sync_course(self):
        sql = """
                select id,
                       course_introduce
                from course_info
        """
        result = self.mysql_reader.read(sql)
        course_ids = [ item['id'] for item in result ]
        introduces = [ item['course_introduce']  for item in result]
        # introduces -> model -> knowledges_list
        knowledges_list = self.extractor.extract(introduces)
        # knowledge_list: [ [k1, k2], [k1, k2]  ]

        knowledge_propetries = []
        # 利用knowledge_propetries[ {'id':xx, 'name':xx}, {}, {} ]
        relations = []
        # 利用relations创建关系
        for course_id, knowledges in zip(course_ids, knowledges_list ):

            for knowledge_index, knowledge in enumerate( knowledges ):

                knowledge_id = '-'.join( [ str(course_id), str(knowledge_index) ] )
                propetry = {'id': knowledge_id, 'name': knowledge  }

                knowledge_propetries.append( propetry )
                rela = {'start_id': course_id, 'end_id': knowledge_id}
                relations.append( rela )
        self.neo4j_writer.write_nodes('knowledge', properties=knowledge_propetries)
        self.neo4j_writer.write_relations(type='Have', start_label='course', end_label='knowledge', relations=relations)

    def sync_question(self):
        sql = """
                select id,
                       question_txt
                from test_question_info
                """
        result = self.mysql_reader.read(sql)
        course_ids = [item['id'] for item in result]
        introduces = [item['question_txt'] for item in result]
        # introduces -> model -> knowledges_list
        knowledges_list = self.extractor.extract(introduces)
        # knowledge_list: [ [k1, k2], [k1, k2]  ]

        knowledge_propetries = []
        # 利用knowledge_propetries[ {'id':xx, 'name':xx}, {}, {} ]
        relations = []
        # 利用relations创建关系
        for course_id, knowledges in zip(course_ids, knowledges_list):

            for knowledge_index, knowledge in enumerate(knowledges):
                knowledge_id = '-'.join([str(course_id), str(knowledge_index)])
                propetry = {'id': knowledge_id, 'name': knowledge}

                knowledge_propetries.append(propetry)
                rela = {'start_id': course_id, 'end_id': knowledge_id}
                relations.append(rela)
        self.neo4j_writer.write_nodes('knowledge', properties=knowledge_propetries)
        self.neo4j_writer.write_relations(type='Have', start_label='question', end_label='knowledge', relations=relations)


if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    # synchronizer.sync_chapter()
    # synchronizer.sync_course()
    synchronizer.sync_question()