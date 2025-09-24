from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_deepseek import ChatDeepSeek
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jVector, Neo4jGraph
from neo4j_graphrag.types import SearchType
from langchain_openai import ChatOpenAI

from configuration import config


class ChatService:
    def __init__(self):
        self.llm = ChatOpenAI(model_name="gpt-4o-mini",openai_api_key="sk-tU7FI6TWAbh2x8HVysdoQmf8L1qK6q6cVKPWJlQBHGTA3Zq7",base_url="https://api.openai-proxy.org/v1")
        self.embedding_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                     encode_kwargs={'normalize_embeddings': True})
        from langchain_community.vectorstores.neo4j_vector import Neo4jVector, SearchType

        # 在 ChatService 的 __init__ 方法中配置
        self.neo4j_vectors = {
            # 1. 分类表（base_category_info）- 无专属向量索引，用【仅关键词检索】
            'base_category_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                keyword_index_name='base_category_full_text_index',  # 专属关键词索引
                index_name='base_category_vector_index',
                search_type=SearchType.HYBRID,
                node_label='base_category_info'  # 明确节点标签，避免歧义
            ),

            # 2. 省份表（base_province）- 无专属向量索引，用【仅关键词检索】
            'base_province': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                keyword_index_name='base_province_full_text_index',  # 专属关键词索引
                index_name='base_province_vector_index',
                search_type=SearchType.HYBRID,
                node_label='base_province'
            ),

            # 3. 学科表（base_subject_info）- 无专属向量索引，用【仅关键词检索】
            'base_subject_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                keyword_index_name='base_subject_full_text_index',  # 专属关键词索引
                index_name='base_subject_placeholder_vector_index',
                search_type=SearchType.HYBRID,
                node_label='base_subject_info'
            ),

            # 4. 课程表（course_info）- 有2个向量索引，按需选择（示例用“课程名称向量”+混合检索）
            'course_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='course_vector_index',  # 专属向量索引（基于课程名称）
                keyword_index_name='course_info_full_text_index',  # 专属关键词索引
                search_type=SearchType.HYBRID,  # 可选混合检索（标签均为 course_info，无匹配问题）
                node_label='course_info'
            ),

            # # 5. 视频表（video_info）- 无专属向量索引，用【仅关键词检索】
            # 'video_info': Neo4jVector.from_existing_index(
            #     self.embedding_model,
            #     url=config.NEO4J_CONFIG["uri"],
            #     username=config.NEO4J_CONFIG['auth'][0],
            #     password=config.NEO4J_CONFIG['auth'][1],
            #     keyword_index_name='video_info_full_text_index',  # 专属关键词索引
            #     index_name='',
            #     search_type=SearchType.HYBRID,
            #     node_label='video_info'
            # ),

            # 6. 题目表（test_question_info）- 有专属向量索引，支持混合/仅向量检索
            'test_question_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='test_question_vector_index',  # 专属向量索引（基于题目内容）
                keyword_index_name='test_question_full_text_index',  # 专属关键词索引
                search_type=SearchType.VECTOR,  # 不强制混合，用仅向量检索（也可改 HYBRID）
                node_label='test_question_info'
            ),

            # # 7. 用户表（user_info）- 无专属向量索引，用【仅关键词检索】
            # 'user_info': Neo4jVector.from_existing_index(
            #     self.embedding_model,
            #     url=config.NEO4J_CONFIG["uri"],
            #     username=config.NEO4J_CONFIG['auth'][0],
            #     password=config.NEO4J_CONFIG['auth'][1],
            #     keyword_index_name='user_info_full_text_index',  # 专属关键词索引
            #     index_name='',
            #     search_type=SearchType.HYBRID,
            #     node_label='user_info'
            # ),

            # 8. 知识点表（knowledge_point）- 有专属向量索引，支持混合/仅向量检索
            'knowledge_point': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='knowledge_point_vector_index',  # 专属向量索引（基于知识点内容）
                keyword_index_name='knowledge_point_full_text_index',  # 专属关键词索引
                search_type=SearchType.VECTOR,  # 仅向量检索（也可改 HYBRID，标签一致）
                node_label='knowledge_point'
            ),

            # 9. 评论表（comment_info）- 有专属向量索引，支持混合/仅向量检索
            'comment_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='comment_vector_index',  # 专属向量索引（基于评论内容）
                # 若后续创建评论关键词索引，可补充 keyword_index_name，此处暂用仅向量
                search_type=SearchType.VECTOR,
                node_label='comment_info'
            )
        }

        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])
        self.json_parser = JsonOutputParser()
        self.str_parser = StrOutputParser()

    def _generate_cypher(self, question):
        prompt = """
                    你是一个专业的Neo4j Cypher查询生成器。你的任务是根据用户问题生成一条Cypher查询语句，用于从知识图谱中获取回答用户问题所需的信息。

                    用户问题：{question}
                    知识图谱结构信息：{schema_info}

                    要求：
                    1. 生成参数化Cypher查询语句，用param_0, param_1等代替具体值,参数前需要加上$符号表示引用
                    2. 识别需要对齐的实体
                    3. 必须严格使用以下JSON格式输出结果
                    {{
                      "cypher_query": "生成的Cypher语句",
                      "entities_to_align": [
                        {{
                          "param_name": "param_0",
                          "entity": "原始实体名称",
                          "label": "节点类型"
                        }}
                      ]
                    }}
                """
        prompt = PromptTemplate.from_template(prompt)
        prompt = prompt.format(question=question, schema_info=self.graph.schema)

        output = self.llm.invoke(prompt)
        result = self.json_parser.invoke(output)
        return result

    def _entity_align(self, entities_to_align):
        for index, entity_to_align in enumerate(entities_to_align):
            label = entity_to_align['label']
            entity = entity_to_align['entity']
            aligned_entity = self.neo4j_vectors[label].similarity_search(entity, k=1)[0].page_content
            entities_to_align[index]['entity'] = aligned_entity
        return entities_to_align

    def chat(self, question):
        # 根据question和图数据库的schema生成Cypher语句以及需要对齐的实体
        result = self._generate_cypher(question)
        cypher = result['cypher_query']
        entities_to_align = result['entities_to_align']
        print(cypher)
        print(entities_to_align)
        # 通过混合检索去做实体对齐
        aligned_entities = self._entity_align(entities_to_align)
        print(aligned_entities)
        # 执行Cypher语句
        query_result = self._execute_query(cypher, aligned_entities)
        print(query_result)
        # 根据用户问题和查询结果生成答案
        answer = self._generate_answer(question, query_result)
        print(answer)
        return answer

    def _execute_query(self, cypher, aligned_entities):
        params = {aligned_entity['param_name']: aligned_entity['entity'] for aligned_entity in aligned_entities}
        print(params)
        return self.graph.query(cypher, params=params)

    def _generate_answer(self, question, query_result):
        prompt = """
        你是一个智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = prompt.format(question=question, query_result=query_result)
        output = self.llm.invoke(prompt)
        return self.str_parser.invoke(output)


if __name__ == '__main__':
    chat_service = ChatService()
    chat_service.chat("魏教师教了哪些课程")
