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
    # index_synchronizer.create_full_index("full_index_question", "Question", "content")
    #
    # index_synchronizer.create_full_index("full_index_knowledge", "Knowledge", "name")
    index_synchronizer.create_full_index("full_index_student", "Student", "id")
