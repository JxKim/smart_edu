from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph, Neo4jVector
from neo4j_graphrag.types import SearchType

from configuration import config


class IndexUtil:
    def __init__(self):
        # 初始化 Neo4j 图数据库连接
        self.graph = Neo4jGraph(
            url=config.NEO4J_CONFIG["uri"],
            username=config.NEO4J_CONFIG['auth'][0],
            password=config.NEO4J_CONFIG['auth'][1]
        )

        # 初始化向量化模型（中文 BGE 模型），用于文本转向量
        # normalize_embeddings=True → 向量归一化，方便做相似度计算（cosine）
        self.embedding_model = HuggingFaceEmbeddings(
            model_name='BAAI/bge-base-zh-v1.5',
            encode_kwargs={'normalize_embeddings': True}
        )

    def create_full_text_index(self, index_name, label, property):
        """
        在 Neo4j 上为某个节点的属性创建全文索引。
        - index_name: 索引名称
        - label: 节点标签
        - property: 节点的属性名
        """
        cypher = f"""
            CREATE FULLTEXT INDEX {index_name} IF NOT EXISTS
            FOR (n:{label}) ON EACH [n.{property}]
        """
        self.graph.query(cypher)

    def create_vector_index(self, index_name, label, source_property, embedding_property):
        """
        在 Neo4j 上为某个节点创建向量索引。
        - index_name: 索引名称
        - label: 节点标签
        - source_property: 原始文本属性（如 name）
        - embedding_property: 存储 embedding 的属性名
        """
        # 先生成 embedding，并写回数据库
        embedding_dim = self._add_embedding(label, source_property, embedding_property)

        # 在 Neo4j 中基于 embedding_property 创建向量索引
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
        """
        给指定 label 的节点计算 embedding，并存储到数据库。
        - label: 节点标签
        - source_property: 用来生成 embedding 的原始文本属性
        - embedding_property: 存 embedding 的属性字段
        """
        # 1. 取出所有该 label 节点的文本和 id
        cypher = f"""
            MATCH (n:{label}) RETURN n.{source_property} AS text,id(n) AS id
        """
        results = self.graph.query(cypher)

        # 2. 取出所有文本
        docs = [result['text'] for result in results]

        # 3. 用 embedding 模型计算文本向量
        embeddings = self.embedding_model.embed_documents(docs)

        # 4. 组装成批量更新的数据
        batch = []
        for result, embedding in zip(results, embeddings):
            item = {
                'id': result['id'],         # Neo4j 节点 id
                'embedding': embedding      # 计算好的向量
            }
            batch.append(item)

        # 5. 将 embedding 写回 Neo4j
        cypher = f"""
            UNWIND $batch AS item
            MATCH (n:{label}) WHERE id(n) = item.id
            SET n.{embedding_property} = item.embedding
        """
        self.graph.query(cypher, params={'batch': batch})

        # 返回向量维度，用于创建索引时指定
        return len(embeddings[0])


if __name__ == '__main__':
    # 初始化工具类
    index_util = IndexUtil ( )

    # 为分类创建全文索引和向量索引
    index_util.create_full_text_index ( "category_full_text_index" , "分类" , "name" )
    index_util.create_vector_index ( "category_vector_index" , "分类" , "name" , "embedding" )

    # 为学科创建全文索引和向量索引
    index_util.create_full_text_index ( "subject_full_text_index" , "学科" , "name" )
    index_util.create_vector_index ( "subject_vector_index" , "学科" , "name" , "embedding" )

    # 为课程创建全文索引和向量索引
    index_util.create_full_text_index ( "course_full_text_index" , "课程" , "name" )
    index_util.create_vector_index ( "course_vector_index" , "课程" , "name" , "embedding" )

    # 为教师创建全文索引和向量索引
    index_util.create_full_text_index ( "teacher_full_text_index" , "教师" , "teacher_name" )
    index_util.create_vector_index ( "teacher_vector_index" , "教师" , "teacher_name" , "embedding" )

    # 为章节创建全文索引和向量索引
    index_util.create_full_text_index ( "chapter_full_text_index" , "章节" , "name" )
    index_util.create_vector_index ( "chapter_vector_index" , "章节" , "name" , "embedding" )

    # 为视频创建全文索引和向量索引
    index_util.create_full_text_index ( "video_full_text_index" , "视频" , "name" )
    index_util.create_vector_index ( "video_vector_index" , "视频" , "name" , "embedding" )

    # 为试卷创建全文索引和向量索引
    index_util.create_full_text_index ( "paper_full_text_index" , "试卷" , "name" )
    index_util.create_vector_index ( "paper_vector_index" , "试卷" , "name" , "embedding" )

    # 为试题创建全文索引和向量索引
    index_util.create_full_text_index ( "question_full_text_index" , "试题" , "name" )
    index_util.create_vector_index ( "question_vector_index" , "试题" , "name" , "embedding" )

    # 为学生创建全文索引和向量索引
    index_util.create_full_text_index ( "student_full_text_index" , "学生" , "nick_name" )
    index_util.create_vector_index ( "student_vector_index" , "学生" , "nick_name" , "embedding" )

    # 为知识点创建全文索引和向量索引
    index_util.create_full_text_index ( "knowledge_full_text_index" , "知识点" , "name" )
    index_util.create_vector_index ( "knowledge_vector_index" , "知识点" , "name" , "embedding" )


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
