from dotenv import load_dotenv

load_dotenv()
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph

from configuration import config


class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(url = config.NEO4J_CONFIG['uri'],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])
        self.embedding_model = HuggingFaceEmbeddings(model_name = 'BAAI/bge-base-zh-v1.5',
                                                     encode_kwargs = {'normalize_embeddings': True})

    def create_full_text_index(self,index_name,label,property):
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS 
            FOR (n:{label}) ON EACH [n.{property}]        
        """
        self.graph.query(cypher)

    def create_vector_index(self,index_name,label,source_property,embedding_property):
        embedding_dim = self._add_embedding(label,source_property,embedding_property)

        cypher = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS 
            FOR (m:{label}) 
            ON m.{embedding_property}
            OPTIONS {{indexConfig:{{`vector.dimensions`:{embedding_dim},
            `vector.similarity_function`:'cosine'}}   
            }}     
        """
        self.graph.query(cypher)

    def _add_embedding(self,label,source_property,embedding_property):
        cypher = f"""
            MATCH (n:{label})
            RETURN n.{source_property} AS text,id(n) AS id
        """
        results = self.graph.query(cypher)
        docs = [result['text'] for result in results]
        embeddings = self.embedding_model.embed_documents(docs)
        batch = []
        for result,embedding in zip(results,embeddings):
            item = {
                'id':result['id'],
                'embedding':embedding
            }
            batch.append(item)
        cypher = f"""
            UNWIND $batch AS item
            MATCH (n:{label}) WHERE id(n) = item.id
            SET n.{embedding_property} = item.embedding
        """
        self.graph.query(cypher,params={'batch':batch})
        return len(embeddings[0])

if __name__ == '__main__':
    index_util = IndexUtil()
    index_util.create_full_text_index('category_full_text_index', '分类', 'name')
    index_util.create_vector_index('category_vector_index', "分类", 'name', "embedding")

    index_util.create_full_text_index('subject_full_text_index', '学科', 'name')
    index_util.create_vector_index('subject_vector_index', "学科", 'name', "embedding")

    index_util.create_full_text_index('course_full_text_index', '课程', 'name')
    index_util.create_vector_index('course_vector_index', "课程", 'name', "embedding")

    index_util.create_full_text_index('test_paper_full_text_index', '试卷', 'name')
    index_util.create_vector_index('test_paper_vector_index', "试卷", 'name', "embedding")

    index_util.create_full_text_index('test_paper_question_full_text_index', '试题', 'name')
    index_util.create_vector_index('test_paper_question_vector_index', "试题", 'name', "embedding")

    index_util.create_full_text_index('chapter_full_text_index', '章节', 'name')
    index_util.create_vector_index('chapter_vector_index', "章节", 'name', "embedding")

    index_util.create_full_text_index('video_full_text_index', '视频', 'name')
    index_util.create_vector_index('video_vector_index', "视频", 'name', "embedding")

    index_util.create_full_text_index('knowledge_point_full_text_index', '知识点', 'name')
    index_util.create_vector_index('knowledge_point_vector_index', "知识点", 'name', "embedding")