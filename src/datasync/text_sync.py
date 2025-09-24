import torch

from datasync.utils import MySqlReader, Neo4jWriter
from models.ner.predict import Predictor


class TextSynchronizer:
    def __init__(self,device):
        self.mysql_reader = MySqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor=Predictor(device)


    def start(self):
        sql='''
        select id,chapter_name as description
        from chapter_info
        '''
        all_id_descs=self.mysql_reader.read(sql)
        max_end=len(all_id_descs)
        start,end=0,min(max_end,100)
        while True:
            id_descs=all_id_descs[start:end]
            ids=[item['id'] for item in id_descs]
            descs=[item['description'] for item in id_descs]
            descs_list=self.extractor.extract(descs)
            properties=[]
            relations=[]
            for idx,descs in zip(ids,descs_list):
                for index,desc in enumerate(descs):
                    tag_id=str(idx)+'-'+str(index)
                    properties.append({'id':tag_id,'name':desc})
                    relations.append({'start_id':idx,'end_id':tag_id})
            self.neo4j_writer.write_nodes('Chapter_knowledge',properties)
            self.neo4j_writer.write_relations('Have','Chapter','Chapter_knowledge',relations)
            if end==max_end:
                break
            start=end
            end=min(max_end,end+100)

if __name__ == '__main__':
    device='cuda' if torch.cuda.is_available() else 'cpu'
    textsynchronizer=TextSynchronizer(device)
    textsynchronizer.start()



