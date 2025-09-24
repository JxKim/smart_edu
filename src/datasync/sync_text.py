import torch
from neo4j import GraphDatabase
from tqdm import tqdm
from transformers import AutoModelForTokenClassification, AutoTokenizer

from conf import config
from datasync.utils import Neo4jWriter, MySqlReader
from models.predict import Predictor


class TextSync:
    def __init__(self,model,tokenizer,device):
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)
        self.predictor=Predictor(model,tokenizer,device)
        self.neo4j_writer=Neo4jWriter()
        self.mysql_reader=MySqlReader()


    def sync_knowledge(self):
        sql="""
        select id,
        chapter_name
        from chapter_info
        """
        chapter_knowledge_point=self.mysql_reader.read_data(sql)
        chapter_knowledge_points=[item['chapter_name'] for item in chapter_knowledge_point]
        chapter_ids=[item['id'] for item in chapter_knowledge_point]
        knowledge_points=[]
        for item in tqdm(chapter_knowledge_points):
            pre=self.predictor.extract(item)
            knowledge_points.append(pre)
        print('预测结束')
        knowledge_properties=[]
        knowledge_relations=[]
        for id, knowledge_point in zip(chapter_ids,knowledge_points):
            for index, knowledge in enumerate(knowledge_point):
                knowledge_id='-'.join([str(id),str(index)])
                knowledge_properties.append({'id':knowledge_id,'KnowledgePoint':knowledge[0] if knowledge else '无'})

                knowledge_relations.append({'start_id':knowledge_id,'end_id':id,'type':'belong'})

        create_node_cypher = f'''
                                unwind $batch as item
                                merge (n:ChapterKnowledgePoint {{id:item.id,name:item.KnowledgePoint}})
                            '''
        create_relationship_cypher = f'''
                                unwind $batch as item
                                match (a:ChapterKnowledgePoint {{id:item.start_id}}),(b:chapter {{id:item.end_id}})
                                merge (a)-[:belong]->(b)
                                merge (b)-[:have]->(a)
                            '''
        self.driver.execute_query(create_node_cypher, batch=knowledge_properties)
        self.driver.execute_query(create_relationship_cypher, batch=knowledge_relations)
        print('关系写入完毕')

if __name__ == '__main__':
    model = AutoModelForTokenClassification.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    tokenizer = AutoTokenizer.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
    text_sync = TextSync(model=model, tokenizer=tokenizer, device=device)
    text_sync.sync_knowledge()








