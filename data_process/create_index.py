from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_neo4j import Neo4jGraph
import os
proxy_url = "http://127.0.0.1:10808"
os.environ['HTTP_PROXY'] = proxy_url
os.environ['HTTPS_PROXY'] = proxy_url
from configuration import config
from dotenv import load_dotenv
import os
load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG['user'],
                                password=config.NEO4J_CONFIG['password'], )

        self.embedding_model = GoogleGenerativeAIEmbeddings(
            model="models/text-embedding-004",
        )

    def create_full_text_index(self, index_name, label, property):
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON EACH [n.{property}]
        """
        self.graph.query(cypher)

    def create_vector_index(self, index_name, label, source_property, embedding_property):
        embedding_dim = self._add_embedding(label, source_property, embedding_property)

        cypher = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
                FOR (m:{label})
                ON m.{embedding_property}
                OPTIONS {{indexConfig: {{
                  `vector.dimensions`: {embedding_dim},
                  `vector.similarity_function`: 'cosine'
                    }}
                }}
        """
        self.graph.query(cypher)

    def _add_embedding(self, label, source_property, embedding_property):
        cypher = f"""
            MATCH (n:{label}) RETURN n.{source_property} AS text,id(n) AS id
        """
        results = self.graph.query(cypher)
        docs = [result['text'] for result in results]
        embeddings = self.embedding_model.embed_documents(docs)

        batch = []
        for result, embedding in zip(results, embeddings):
            item = {
                'id': result['id'],
                'embedding': embedding
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
    # 创建索引
    index_util = IndexUtil()
    # index_util.create_full_text_index("category_full_text_index", "Category", "name")
    # index_util.create_vector_index("category_vector_index", "Category", "name", "embedding")
    #
    # index_util.create_full_text_index("subject_full_text_index", "Subject", "name")
    # index_util.create_vector_index("subject_vector_index", "Subject", "name", "embedding")
    #
    # index_util.create_full_text_index("course_full_text_index", "Course", "name")
    # index_util.create_vector_index("course_vector_index", "Course", "name", "embedding")
    #
    # index_util.create_full_text_index("teacher_full_text_index", "Teacher", "name")
    # index_util.create_vector_index("teacher_vector_index", "Teacher", "name", "embedding")
    #
    # index_util.create_full_text_index("chapter_full_text_index", "Chapter", "name")
    # index_util.create_vector_index("chapter_vector_index", "Chapter", "name", "embedding")

    index_util.create_full_text_index("video_full_text_index", "Video", "name")
    index_util.create_vector_index("video_vector_index", "Video", "name", "embedding")

    index_util.create_full_text_index("paper_full_text_index", "Paper", "title")
    index_util.create_vector_index("paper_vector_index", "Paper", "title", "embedding")

    index_util.create_full_text_index("question_full_text_index", "Question", "text")
    index_util.create_vector_index("question_vector_index", "Question", "text", "embedding")

    index_util.create_full_text_index("knowledge_point_full_text_index", "KnowledgePoint", "name")
    index_util.create_vector_index("knowledge_point_vector_index", "KnowledgePoint", "name", "embedding")

    index_util.create_full_text_index("user_full_text_index", "User", "name")
    index_util.create_vector_index("user_vector_index", "User", "name", "embedding")
    # #
