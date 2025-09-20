from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph

from config import config


class IndexUtil:
    def __init__(self):
        self.grap=Neo4jGraph(
            url=config.NEO4J_CONFIG['uri'],
            username=config.NEO4J_CONFIG["auth"][0],
            password=config.NEO4J_CONFIG["auth"][1]
        )

        # self.embedding_model=HuggingFaceEmbeddings(
        #     model_name="BAAI/bge-base-zh-v1.5",
        #     encode_kwargs={'normalize_embeddings':True}
        # )

    # def create_full_text_index(self):

if __name__=="__main__":
    util=IndexUtil()

