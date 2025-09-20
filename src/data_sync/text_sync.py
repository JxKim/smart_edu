from datasets import load_dataset

from config import config
from data_sync.utils import MySqlReader, Neo4jWriter


class TextSync:
    def __init__(self):
        sql_reader=MySqlReader()
        neo4j_writer=Neo4jWriter()

    def parse_json(self,path):
        dataset = load_dataset("json",data_files=path)['train']
        print(dataset)
        labels=dataset['label']
        for label_dict in labels:
            print(label_dict)
        labels = [label_dict[0]['text'] for label_dict in labels]
        print(labels)

if __name__=="__main__":
    text_sync=TextSync()
    text_sync.parse_json(str(config.DATA_DIR/'data.json'))
