from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph

from configuration import config


class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(
            url=config.NEO4J_CONFIG['uri'],
            username=config.NEO4J_CONFIG['auth'][0],
            password=config.NEO4J_CONFIG['auth'][1]
        )
        self.embedding_model = HuggingFaceEmbeddings(
            model_name='BAAI/bge-base-zh-v1.5',
            encode_kwargs = {'normalize_embeddings': False}
        )

    def create_full_text_index(self,index_name,label,property):
        cypher=f"""
        create FULLTEXT INDEX {index_name} if not exists
        FOR (n:{label}) ON EACH [n.{property}]
        """
        self.graph.query(cypher)
    def create_vector_index(self,index_name,label,source_property,embedding_property):
        embedding_dim=self._add_embedding(label,source_property,embedding_property)
        cypher=f"""
                create VECTOR INDEX {index_name} if not exists
                for (m:{label})
                on m.{embedding_property}
                OPTIONS {{indexConfig: {{
                  `vector.dimensions`: {embedding_dim},
                  `vector.similarity_function`: 'cosine'
                    }}
                }}
        """
        self.graph.query(cypher)

    def _add_embedding(self,label,source_property,embedding_property):
        cypher=f"""
match (n:{label}) return n.{source_property} AS text,id(n) AS id"""
        results=self.graph.query(cypher)
        docs=[result['text'] for result in results]
        embeddings=self.embedding_model.embed_documents(docs)#返回列表的列表

        batch=[]
        for result,embedding in zip(results,embeddings):
            if embedding:
                item={
                    'id':result['id'],
                    'embedding':embedding,
                }
                batch.append(item)

        cypher=f"""
UNWIND $batch AS item
match (n:{label}) where id(n)=item.id
set n.{embedding_property}=item.embedding"""
        self.graph.query(cypher,params={'batch':batch})
        return len(embeddings[0])

if __name__ == '__main__':
    # 创建索引
    index_util = IndexUtil()
    index_util.create_full_text_index("base_category_info_full_text_index", "base_category_info", "category_name")
    index_util.create_vector_index("base_category_info_vector_index", "base_category_info", "category_name", "embedding")

    index_util.create_full_text_index("base_province_full_text_index", "base_province", "name")
    index_util.create_vector_index("base_province_vector_index", "base_province", "name", "embedding")

    index_util.create_full_text_index("base_subject_info_full_text_index", "base_subject_info", "subject_name")
    index_util.create_vector_index("base_subject_info_vector_index", "base_subject_info", "subject_name", "embedding")

    index_util.create_full_text_index("cart_info_full_text_index", "cart_info", "course_name")
    index_util.create_vector_index("cart_info_vector_index", "cart_info", "course_name", "embedding")

    index_util.create_full_text_index("chapter_info_full_text_index", "chapter_info", "chapter_name")
    index_util.create_vector_index("chapter_info_vector_index", "chapter_info", "chapter_name", "embedding")

    index_util.create_full_text_index("course_info_full_text_index", "course_info", "course_name")
    index_util.create_vector_index("course_info_vector_index", "course_info", "course_name", "embedding")

    index_util.create_full_text_index("knowledge_point_full_text_index", "knowledge_point", "point_txt")
    index_util.create_vector_index("knowledge_point_vector_index", "knowledge_point", "point_txt", "embedding")

    index_util.create_full_text_index("order_detail_full_text_index", "order_detail", "course_name")
    index_util.create_vector_index("order_detail_vector_index", "order_detail", "course_name", "embedding")

    index_util.create_full_text_index("test_paper_full_text_index", "test_paper", "paper_title")
    index_util.create_vector_index("test_paper_vector_index", "test_paper", "paper_title", "embedding")

    index_util.create_full_text_index("test_question_info_full_text_index", "test_question_info", "question_txt")
    index_util.create_vector_index("test_question_info_vector_index", "test_question_info", "question_txt", "embedding")

    index_util.create_full_text_index("video_info_full_text_index", "video_info", "video_name")
    index_util.create_vector_index("video_info_vector_index", "video_info", "video_name", "embedding")
