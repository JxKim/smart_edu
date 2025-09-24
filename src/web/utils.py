from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph, Neo4jVector
from neo4j_graphrag.types import SearchType

from configuration import config


class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])

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
        # 过滤并处理文本
        valid_docs = []
        for doc in docs:
            if doc is None:
                valid_docs.append("")  # 替换 None 为空字符串
            elif not isinstance(doc, str):
                valid_docs.append(str(doc))  # 转换为字符串
            else:
                valid_docs.append(doc)

        # 处理换行符
        processed_docs = [doc.replace("\n", " ") for doc in valid_docs]

        # 生成嵌入
        embeddings = self.embedding_model.embed_documents(processed_docs)

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
    print("初始化完成")


    # # 1. 分类表（base_category_info）- 按分类名称检索
    # index_util.create_full_text_index(
    #     index_name="base_category_full_text_index",  # 索引名（唯一）
    #     label="base_category_info",  # 节点标签（表名）
    #     property="category_name"  # 检索字段（分类名称）
    # )
    # # 2. 省份表（base_province）- 按省份名称检索
    # index_util.create_full_text_index(
    #     index_name="base_province_full_text_index",
    #     label="base_province",
    #     property="name"  # 省份表中文本字段为"name"
    # )
    #
    # # 3. 学科表（base_subject_info）- 按学科名称检索
    # index_util.create_full_text_index(
    #     index_name="base_subject_full_text_index",
    #     label="base_subject_info",
    #     property="subject_name"
    # )
    #
    # # 4. 课程表（course_info）- 按课程名称检索（核心检索字段）
    # index_util.create_full_text_index(
    #     index_name="course_info_full_text_index",
    #     label="course_info",
    #     property="course_name"
    # )
    #
    # # 5. 视频表（video_info）- 按视频名称检索
    # index_util.create_full_text_index(
    #     index_name="video_info_full_text_index",
    #     label="video_info",
    #     property="video_name"
    # )
    #
    # # 6. 题目表（test_question_info）- 按题目内容检索（核心检索字段）
    # index_util.create_full_text_index(
    #     index_name="test_question_full_text_index",
    #     label="test_question_info",
    #     property="question_txt"  # 题目内容为长文本，适合全文检索
    # )
    #
    # # 7. 用户表（user_info）- 按用户昵称/用户名检索
    # index_util.create_full_text_index(
    #     index_name="user_info_full_text_index",
    #     label="user_info",
    #     property="nick_name"  # 优先用昵称检索，也可选择"login_name"
    # )
    #
    # # 8. 知识点表（knowledge_point）- 按知识点内容检索
    # index_util.create_full_text_index(
    #     index_name="knowledge_point_full_text_index",
    #     label="knowledge_point",
    #     property="point_txt"
    # )
    #
    # # 1. 课程表（course_info）- 基于课程名称生成向量，用于“相似课程推荐”
    # index_util.create_vector_index(
    #     index_name="course_vector_index",  # 向量索引名（唯一）
    #     label="course_info",  # 节点标签
    #     source_property="course_name",  # 生成向量的源文本字段（课程名称）
    #     embedding_property="course_embedding"  # 存储向量的字段名（自定义，避免冲突）
    # )
    #
    # # 2. 题目表（test_question_info）- 基于题目内容生成向量，用于“相似题目推荐”
    # index_util.create_vector_index(
    #     index_name="test_question_vector_index",
    #     label="test_question_info",
    #     source_property="question_txt",  # 题目内容为核心语义字段
    #     embedding_property="question_embedding"
    # )
    #
    # # 3. 知识点表（knowledge_point）- 基于知识点内容生成向量，用于“相似知识点关联”
    # index_util.create_vector_index(
    #     index_name="knowledge_point_vector_index",
    #     label="knowledge_point",
    #     source_property="point_txt",
    #     embedding_property="point_embedding"
    # )
    # #
    # # 4. 课程介绍（course_info）- 基于课程详情生成向量（如需更精准的课程语义匹配）
    # index_util.create_vector_index(
    #     index_name="course_intro_vector_index",
    #     label="course_info",
    #     source_property="course_introduce",  # 课程介绍为长文本，语义更丰富
    #     embedding_property="intro_embedding"
    # )
    #
    # # 5. 用户评论（comment_info）- 基于评论内容生成向量，用于“评论语义分析”
    # index_util.create_vector_index(
    #     index_name="comment_vector_index",
    #     label="comment_info",
    #     source_property="comment_txt",
    #     embedding_property="comment_embedding"
    # )
    #
    #
    # # 查询索引
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
    # 在创建索引的代码中，新增分类表的占位向量索引
    # index_util.create_vector_index(
    #     index_name="base_category_vector_index",  # 分类表专属占位向量索引
    #     label="base_category_info",  # 与关键词索引的标签一致
    #     source_property="category_name",  # 可使用相同的文本字段
    #     embedding_property="category_embedding"  # 占位字段（无需实际生成向量）
    # )
    #
    # # 同理，为省份表创建占位向量索引
    # index_util.create_vector_index(
    #     index_name="base_province_vector_index",
    #     label="base_province",
    #     source_property="name",
    #     embedding_property="province_embedding"
    # )
    index_util.create_vector_index(
        index_name="base_subject_placeholder_vector_index",  # 索引名（唯一标识）
        label="base_subject_info",  # 节点标签：必须与学科表的关键词索引标签一致
        source_property="subject_name",  # 源文本字段（与关键词索引的检索字段一致）
        embedding_property="subject_placeholder_embedding"  # 占位向量字段（无需实际生成向量值）
    )
