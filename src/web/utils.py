import os

from dotenv import load_dotenv
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph
import dotenv
load_dotenv()

neo4j_nri = os.getenv('NEO4J_NRI')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

class IndexCreateTool:
    def __init__(self):
        self.graph = Neo4jGraph(url=neo4j_nri, username=neo4j_user, password=neo4j_password)
        self.embedding_model = HuggingFaceEmbeddings(
            model_name='BAAI/bge-base-zh-v1.5',
            encode_kwargs={"normalize_embeddings": True}
        )
    def create_full_text_index(self, index_name, label, property_name):
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON EACH [n.{property_name}]
        """
        self.graph.query(cypher)
    def create_vector_index(self, index_name, label, source_property, embedding_property):
        embedding_dim = self._add_embedding_property(label, source_property, embedding_property)
        cypher = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON (n.{embedding_property})
            OPTIONS {{indexConfig: {{
                `vector.dimensions`: {embedding_dim},
                `vector.similarity_Function`: 'cosine'
            }}
             }}
        """
        self.graph.query(cypher)

    def _add_embedding_property(self, label, source_property, embedding_property):
        cypher = f"""
            MATCH (n:{label})
            RETURN n.{source_property} AS text, id(n) AS node_id
        """
        datas = self.graph.query(cypher)
        docs = [doc['text'] for doc in datas]
        try:
            embeddings = self.embedding_model.embed_documents(docs)
        except Exception as e:
            print(docs)
        # print(embeddings[0])
        batch = []
        for data, embedding in zip(datas, embeddings):
            item = {
                "id": data['node_id'],
                "embedding": embedding
            }
            batch.append(item)
        cypher = f"""
                UNWIND $batch AS item
                MATCH (n:{label}) WHERE id(n) = item.id
                SET n.{embedding_property} = item.embedding
                """
        self.graph.query(cypher, params={'batch': batch})
        return len(embeddings[0])
if __name__ == '__main__':
    create_index = IndexCreateTool()
    # create_index.create_full_text_index('course_full_text_index', 'Course', 'name')
    # create_index.create_vector_index('course_vector_index', 'Course', 'name', 'embedding')
    # create_index.create_full_text_index('category_full_text_index', 'Category', 'name')
    # create_index.create_vector_index('category_vector_index', 'Category', 'name', 'embedding')
    # create_index.create_full_text_index('subject_full_text_index', 'Subject', 'name')
    # create_index.create_vector_index('subject_vector_index', 'Subject', 'name', 'embedding')
    # create_index.create_full_text_index('chapter_full_text_index', 'Chapter', 'name')
    # create_index.create_vector_index('chapter_vector_index', 'Chapter', 'name', 'embedding')
    # create_index.create_full_text_index('knowledge_point_full_text_index', 'KnowledgePoint', 'name')
    create_index.create_vector_index('knowledge_point_vector_index', 'KnowledgePoint', 'txt', 'embedding')
    # create_index.create_full_text_index('paper_full_text_index', 'ExamPaper', 'title')
    # create_index.create_vector_index('paper_vector_index', 'ExamPaper', 'title', 'embedding')
    # create_index.create_full_text_index('question_full_text_index', 'Question', 'question')
    # create_index.create_vector_index('question_vector_index', 'Question', 'question', 'embedding')
    # create_index.create_full_text_index('teacher_full_text_index', 'Teacher', 'name')
    # create_index.create_vector_index('teacher_vector_index', 'Teacher', 'name', 'embedding')
    # create_index.create_full_text_index('user_full_text_index', 'User', 'login_name')
    # create_index.create_vector_index('user_vector_index', 'User', 'login_name', 'embedding')