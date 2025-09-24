from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph

from configuration import config


class IndexUtil:
    #初始化方法
    def __init__(self):
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG["auth"][0],
                                password=config.NEO4J_CONFIG["auth"][1])
        self.embedding_model = HuggingFaceEmbeddings(model_name="BAAI/bge-base-zh-v1.5",
                                                     encode_kwargs = {"normalize_embeddings": True})
    #创建全文索引方法
    def create_full_text_index(self, index_name,label,property):
        cypher = f"""
                CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
                FOR (n:{label}) ON EACH [n.{property}]        
        """
        self.graph.query(cypher)

    #创建向量索引方法
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

    #添加嵌入向量的私有方法
    def _add_embedding(self, label, source_property, embedding_property):
        cypher = f"""
            MATCH (n:{label}) RETURN n.{source_property} AS text,id(n) AS id
        """
        results = self.graph.query(cypher)
        docs = [result['text'] for result in results]
        embeddings = self.embedding_model.embed_documents(docs)

        batch = []
        for result,embedding in zip(results,embeddings):
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
        self.graph.query(cypher,params = {'batch': batch})
        return len(embeddings[0])


if __name__ == '__main__':
    # 创建索引
    index_util = IndexUtil()
    index_util.create_full_text_index("subject_full_text_index", "Subject", "name")
    index_util.create_vector_index("subject_vector_index", "Subject", "name", "embedding")

    index_util.create_full_text_index("course_full_text_index", "Course", "name")
    index_util.create_vector_index("course_vector_index", "Course", "name", "embedding")

    index_util.create_full_text_index("teacher_full_text_index", "Teacher", "name")
    index_util.create_vector_index("teacher_vector_index", "Teacher", "name", "embedding")

    index_util.create_full_text_index("price_full_text_index", "Price", "name")
    index_util.create_vector_index("price_vector_index", "Price", "name", "embedding")

    index_util.create_full_text_index("chapter_full_text_index", "Chapter", "name")
    index_util.create_vector_index("chapter_vector_index", "Chapter", "name", "embedding")

    index_util.create_full_text_index("paper_full_text_index", "Paper", "name")
    index_util.create_vector_index("paper_vector_index", "Paper", "name", "embedding")

    index_util.create_full_text_index("video_full_text_index", "Video", "name")
    index_util.create_vector_index("video_vector_index", "Video", "name", "embedding")

    index_util.create_full_text_index("question_full_text_index", "Question", "name")
    index_util.create_vector_index("question_vector_index", "Question", "name", "embedding")