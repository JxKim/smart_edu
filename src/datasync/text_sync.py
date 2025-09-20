import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter
from models.ner.predict import Predictor


class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extract()

    def _init_extract(self):
        model = AutoModelForTokenClassification.from_pretrained(str(config.CHECK_POINT / 'ner' / 'best_model'))
        tokenizer = AutoTokenizer.from_pretrained(str(config.CHECK_POINT / 'ner' / 'best_model'))
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        return Predictor(model, tokenizer, device)

    def sync_spu_tag(self):
        """
        写入spu标签信息
        :return:
        """
        sql = """
            select id,
                   description
            from spu_info
        """
        spu_desc = self.mysql_reader.read(sql)
        ids = [spu['id'] for spu in spu_desc]
        tags_list = [spu['description'] for spu in spu_desc]
        tags_list = self.extractor.extract(tags_list)

        properties, relations = [], []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                tag_id = str(id) + '-' + str(index)
                property = {
                    'id': tag_id,
                    'name': tag
                }
                properties.append(property)
                relation = {
                    'start_id': id,
                    'end_id': tag_id
                }
                relations.append(relation)
        self.neo4j_writer.write_nodes('Tag', properties)
        self.neo4j_writer.write_relations('Have', 'SPU', 'Tag', relations)

    def sync_sku_tag(self):
        """
        写入sku标签信息
        :return:
        """
        sql = """
            select id,
                   sku_desc description
            from sku_info
        """
        sku_desc = self.mysql_reader.read(sql)
        ids = [sku['id'] for sku in sku_desc]
        tags_list = [sku['description'] for sku in sku_desc]
        tags_list = self.extractor.extract(tags_list)

        properties, relations = [], []
        for id, tags in zip(ids, tags_list):
            for index, tag in enumerate(tags):
                tag_id = str(id) + '-' + str(index)
                property = {
                    'id': tag_id,
                    'name': tag
                }
                properties.append(property)
                relation = {
                    'start_id': id,
                    'end_id': tag_id
                }
                relations.append(relation)
        self.neo4j_writer.write_nodes('Tag', properties)
        self.neo4j_writer.write_relations('Have', 'SKU', 'Tag', relations)


if __name__ == '__main__':
    ts = TextSynchronizer()
    ts.sync_spu_tag()
    ts.sync_sku_tag()
