from langchain_neo4j import Neo4jGraph, Neo4jVector
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_neo4j.vectorstores.neo4j_vector import SearchType
from langchain_openai import OpenAIEmbeddings

from src.configuration import config


class IndexUtili:
    def __init__(self):
        self.graph=Neo4jGraph(url=config.NEO4J_CONFIG['uri'],
                   username=config.NEO4J_CONFIG['auth'][0],
                   password=config.NEO4J_CONFIG['auth'][1]
                   )
        self.embed_model = HuggingFaceEmbeddings(
                                    model_name="BAAI/bge-base-zh-v1.5",
                                    model_kwargs={'device': 'cuda'},
                                    encode_kwargs= {'normalize_embeddings': True} ,
                                    )
    def create_full_text_index(self,index_name,label,properties):
        cypher=f'''
        CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
        FOR (n:{label}) ON EACH [n.{properties}]
        '''
        self.graph.query(cypher)

    def create_vector_index(self,index_name,label,properties):
        vector_dim=self._set_embedding(label,properties)
        cypher=f'''
        CREATE VECTOR INDEX  {index_name} IF NOT EXISTS
        FOR (m:{label})
        ON m.embedding
        OPTIONS {{ indexConfig:{{
         `vector.dimensions`: {vector_dim},
         `vector.similarity_function`: 'cosine'
        }}}}
        '''
        self.graph.query(cypher)

    def _set_embedding(self,label,properties):
        results=self.graph.query(f'match (n:{label}) return n.{properties} as text ,elementId(n) as id')
        texts=[result['text'] for result in results]
        ids=[result['id'] for result in results]
        vectors=self.embed_model.embed_documents(texts)
        batch=[]
        for idx,vector in zip(ids,vectors):
            batch.append({'id':idx,'vector':vector})
        cypher=f'''
        unwind $batch as batch
        match (n:{label}) where elementId(n)=batch.id
        set n.embedding = batch.vector
        '''
        self.graph.query(cypher, {'batch' : batch})
        return len(vectors[0])



if __name__ == '__main__':
    index_util=IndexUtili()
    index_util.create_vector_index('Base_category_v','Base_category','name')
    index_util.create_full_text_index('Base_category_f','Base_category','name')

    index_util.create_vector_index('Course_v', 'Course', 'name')
    index_util.create_full_text_index('Course_f', 'Course', 'name')

    index_util.create_vector_index('Base_subject_v', 'Base_subject', 'name')
    index_util.create_full_text_index('Base_subject_f', 'Base_subject', 'name')

    index_util.create_vector_index('Test_paper_v', 'Test_paper', 'name')
    index_util.create_full_text_index('Test_paper_f', 'Test_paper', 'name')


    embed_model = HuggingFaceEmbeddings(
        model_name="BAAI/bge-base-zh-v1.5",
        model_kwargs={'device': 'cuda'},
        encode_kwargs={'normalize_embeddings': True},
    )
    store = Neo4jVector.from_existing_index(
        embed_model,
        url=config.NEO4J_CONFIG['uri'],
        username=config.NEO4J_CONFIG['auth'][0],
        password=config.NEO4J_CONFIG['auth'][1],
        index_name='Base_subject_v',
        keyword_index_name='Base_subject_f',
        search_type=SearchType.HYBRID,
        )
    print(store.similarity_search('嵌入物联', k=2))


