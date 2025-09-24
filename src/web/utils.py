import os

from dotenv import load_dotenv
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph, Neo4jVector
from neo4j_graphrag.types import SearchType
load_dotenv()
from configuration import config

#建立索引

class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(os.getenv('NEO4J_URI'),username=os.getenv('NEO4J_AUTH_USER'),password=os.getenv('NEO4J_AUTH_PASSWORD'))

        self.embedding_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                     encode_kwargs={'normalize_embeddings': True})

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


def main():
    # 创建索引
    index_util = IndexUtil()

    #非结构化建立索引
    print('非结构化建立索引')
    index_util.create_full_text_index("tag_full_text_index", "Tag", "name")
    index_util.create_vector_index("tag_vector_index", "Tag", "name", "embedding")

    #创建category索引
    print('创建category分类索引')
    index_util.create_full_text_index("category_full_text_index", "base_category_info", "category_name")
    index_util.create_vector_index("category_vector_index", "base_category_info", "category_name", "embedding")

    #创建subject索引
    print('创建subject索引')
    index_util.create_full_text_index("subject_full_text_index", "base_subject_info", "subject_name")
    index_util.create_vector_index("subject_vector_index", "base_subject_info", "subject_name", "embedding")

    #chapter索引
    print('创建chapter索引')
    index_util.create_full_text_index("chapter_full_text_index", "chapter_info", "chapter_name")
    index_util.create_vector_index("chapter_vector_index", "chapter_info", "chapter_name", "embedding")

    #course索引
    print('创建course索引')
    index_util.create_full_text_index("course_full_text_index", "course_info", "course_name")
    index_util.create_vector_index("course_vector_index", "course_info", "course_name", "embedding")

    #knowledge_point
    print('创建knowledge_point索引')
    index_util.create_full_text_index("knowledge_point_full_text_index", "knowledge_point", "point_txt")
    index_util.create_vector_index("knowledge_point_vector_index", "knowledge_point", "point_txt", "embedding")

    #test_paper
    print('创建test_paper索引')
    index_util.create_full_text_index("test_paper_full_text_index", "test_paper", "paper_title")
    index_util.create_vector_index("test_paper_vector_index", "test_paper", "paper_title", "embedding")

    #test_question_info
    print('创建test_question_info索引')
    index_util.create_full_text_index("test_question_info_full_text_index", "test_question_info", "question_txt")
    index_util.create_vector_index("test_question_info_vector_index", "test_question_info", "question_txt", "embedding")

    #video_info索引
    print('创建video_info索引')
    index_util.create_full_text_index("video_info_full_text_index", "video_info", "video_name")
    index_util.create_vector_index("video_info_vector_index", "video_info", "video_name", "embedding")


    # index_util.create_full_text_index("trademark_full_text_index", "Trademark", "name")
    # index_util.create_vector_index("trademark_vector_index", "Trademark", "name", "embedding")
    #
    # index_util.create_full_text_index("spu_full_text_index", "SPU", "name")
    # index_util.create_vector_index("spu_vector_index", "SPU", "name", "embedding")
    #
    # index_util.create_full_text_index("sku_full_text_index", "SKU", "name")
    # index_util.create_vector_index("sku_vector_index", "SKU", "name", "embedding")
    #
    # index_util.create_full_text_index("category1_full_text_index", "Category1", "name")
    # index_util.create_vector_index("category1_vector_index", "Category1", "name", "embedding")
    #
    # index_util.create_full_text_index("category2_full_text_index", "Category2", "name")
    # index_util.create_vector_index("category2_vector_index", "Category2", "name", "embedding")
    #
    # index_util.create_full_text_index("category3_full_text_index", "Category3", "name")
    # index_util.create_vector_index("category3_vector_index", "Category3", "name", "embedding")

    # 查询索引
    # index_name = "trademark_vector_index"  # default index name
    # keyword_index_name = "trademark_full_text_index"  # default keyword index name
    #
    # embedding_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
    #                                         encode_kwargs={'normalize_embeddings': True})
    # store = Neo4jVector.from_existing_index(
    #     embedding_model,
    #     url=config.NEO4J_CONFIG["uri"],
    #     username=config.NEO4J_CONFIG['auth'][0],
    #     password=config.NEO4J_CONFIG['auth'][1],
    #     index_name=index_name,
    #     keyword_index_name=keyword_index_name,
    #     search_type=SearchType.HYBRID,
    # )
    #
    # print(store.similarity_search("Apple", k=1)[0].page_content)



if __name__ == '__main__':
    # 创建索引
    index_util = IndexUtil()
    index_util.create_full_text_index("category_full_text_index", "base_category_info", "category_name")
