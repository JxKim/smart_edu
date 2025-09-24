import torch
# from tenacity import sleep_using_event
from transformers import AutoModelForTokenClassification,AutoTokenizer

from configuration import config
from model_src.uie.uie_predictor import UIEPredictor
from datasync.utils_data import MysqlReader,Neo4jWriter



class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extractor()

    def _init_extractor(self):
        model = AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR/'ner'/'best_model')
        tokenizer = AutoTokenizer.from_pretrained(config.CHECKPOINT_DIR/'ner'/'best_model')
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        schema=['Tag']
        ie = UIEPredictor ( schema=schema , model="uie-m-base" , schema_lang="en" )
        def Predictor(model,tokenizer,device):
            def predict(texts):
                return ie.run_on_texts(texts,model,tokenizer,device)
            return predict
        return Predictor(model,tokenizer,device)


    def sync_tag(self):
        sql = """
            select id,
                description
            from spu_info
            """
        spu_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in spu_desc]
        descs = [item['description'] for item in spu_desc]
        tags_list = self.extractor.predict(descs)

        tag_properties = []
        relations = []
        for id,tags in zip(ids,tags_list):
            for index,tag in enumerate(tags):
                tag_id = '-'.join([str(id),str(index)])
                property = {'id':tag_id,'name':tag}
                tag_properties.append(property)
                relation = ({'start_id':id,'end_id':tag_id})
                relations.append(relation)
        self.neo4j_writer.write_nodes("Tag",tag_properties)
        self.neo4j_writer.write_relations("Have","Spu","Tag",relations)


if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    synchronizer.sync_tag()