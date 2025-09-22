from dotenv import load_dotenv
load_dotenv()

from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph


class IndexSynchronizer:
    def __init__(self):
        self.graph = Neo4jGraph()
        self.embedding_model = HuggingFaceEmbeddings(model_name = "BAAI/bge-base-zh-v1.5",
                                                     encode_kwargs ={"normalize_embeddings": True})

    def create_full_index(self,index_name,label,property):
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON EACH [n.{property}]
        """
        self.graph.query(cypher)

    def _add_embedding(self,label,source_property,embedding_property):
        cypher = f"""
            MATCH (n:{label}) RETURN n.{source_property} AS text,id(n) AS id
        """
        results = self.graph.query(cypher)
        docs = [result['text'] for result in results]
        embeddings = self.embedding_model.embed_documents(docs)

        batch = []
        for result,embedding in zip(results,embeddings):
            item =  {
                "id":result['id'],
                'embedding':embedding
            }
            batch.append(item)
        cypher = f"""
                    UNWIND $batch AS item
                    MATCH (n:{label}) WHERE id(n) = item.id
                    SET n.{embedding_property} = item.embedding
                """
        self.graph.query(cypher, params={'batch': batch})
        return len(embeddings[0])

    def create_vector_index(self,index_name,label,source_property,embedding_property):
        embedding_dim = self._add_embedding(label,source_property,embedding_property)
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


if __name__ == "__main__":
    index_synchronizer = IndexSynchronizer()
    # index_synchronizer.create_full_index("full_index_category", "Category", "name")
    # index_synchronizer.create_full_index("full_index_subject", "Subject", "name")
    # index_synchronizer.create_full_index("full_index_course", "Course", "name")
    # index_synchronizer.create_full_index("full_index_teacher", "Teacher", "name")
    # index_synchronizer.create_full_index("full_index_price", "Price", "name")
    # index_synchronizer.create_full_index("full_index_chapter", "Chapter", "name")
    # index_synchronizer.create_full_index("full_index_video", "Video", "name")
    # index_synchronizer.create_full_index("full_index_paper", "Paper", "name")
    # index_synchronizer.create_full_index("full_index_question", "Question", "name")
    #
    # index_synchronizer.create_full_index("full_index_knowledge", "Knowledge", "name")
    # index_synchronizer.create_full_index("full_index_student", "Student", "id")

    # 向量索引
    # index_synchronizer.create_vector_index("vector_index_category", "Category", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_subject", "Subject", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_course", "Course", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_teacher", "Teacher", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_price", "Price", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_chapter", "Chapter", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_video", "Video", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_paper", "Paper", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_question", "Question", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_knowledge", "Knowledge", "name", "embedding")
    # index_synchronizer.create_vector_index("vector_index_student", "Student", "id", "embedding")