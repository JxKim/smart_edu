import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph, Neo4jVector
from neo4j_graphrag.types import SearchType
from configuration import config

# 创建向量索引和全文索引

class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])

        self.embedding_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                     encode_kwargs={'normalize_embeddings': True})


    def create_full_text_index(self, index_name, label, property):
        # IF NOT EXISTS 确保索引不存在时才创建，避免重复创建错误
        # index_util.create_full_text_index(index_name="trademark_full_text_index", label="Trademark", property="name")
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON EACH [n.{property}]
        """
        self.graph.query(cypher)

    def create_vector_index(self, index_name, label, source_property, embedding_property):
        #create_vector_index("trademark_vector_index", "Trademark", "name", "embedding")
        embedding_dim = self._add_embedding(label, source_property, embedding_property)

        # 向量嵌入：
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
            MATCH (n:{label}) RETURN n.{source_property} AS text, id(n) AS id
        """
        results = self.graph.query(cypher)

        docs = [result['text'] for result in results]

        # 分批处理，避免GPU超时
        batch_size = 16  # 根据你的GPU内存调整
        embeddings = []
        for i in range(0, len(docs), batch_size):
            batch_docs = docs[i:i + batch_size]
            batch_embeddings = self.embedding_model.embed_documents(batch_docs)
            embeddings.extend(batch_embeddings)

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
        return len(embeddings[0]) if embeddings else 0

if __name__ == '__main__':
    # 创建索引
    index_util = IndexUtil()
    # index_util.create_full_text_index("category_full_text_index", "category", "name")
    # index_util.create_vector_index("category_vector_index", "category", "name", "embedding")
    #
    # index_util.create_full_text_index("subject_full_text_index", "subject", "name")
    # index_util.create_vector_index("subject_vector_index", "subject", "name", "embedding")
    #
    # index_util.create_full_text_index("course_full_text_index", "course", "name")
    # index_util.create_vector_index("course_vector_index", "course", "name", "embedding")
    #
    # index_util.create_full_text_index("chapter_full_text_index", "chapter", "name")
    # index_util.create_vector_index("chapter_vector_index", "chapter", "name", "embedding")
    #
    # # 试卷
    # index_util.create_full_text_index("paper_full_text_index", "paper", "name")
    # index_util.create_vector_index("paper_vector_index", "paper", "name", "embedding")

    # 试题
    index_util.create_full_text_index("question_full_text_index", "question", "name")
    index_util.create_vector_index("question_vector_index", "question", "name", "embedding")

    # 知识点
    index_util.create_full_text_index("knowledge_full_text_index", "knowledge", "name")
    index_util.create_vector_index("knowledge_vector_index", "knowledge", "name", "embedding")

    print('Finished')

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
    #     index_name=index_name, # 向量索引名称
    #     keyword_index_name=keyword_index_name, #　全文索引名称
    #     search_type=SearchType.HYBRID,
    # )
    #
    # print(store.similarity_search("数据库", k=1)[0].page_content)
