import dotenv
dotenv.load_dotenv()
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
# from langchain_deepseek import ChatDeepSeek
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jVector, Neo4jGraph
from langchain_openai import ChatOpenAI
from neo4j_graphrag.types import SearchType

from configuration import config


class ChatService:
    def __init__(self):
        self.llm = ChatOpenAI(model_name='gpt-4o-mini')
        self.embedding_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                     encode_kwargs={'normalize_embeddings': True})
        self.neo4j_vectors = {
            'base_category_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG['uri'],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='base_category_info_vector_index',
                keyword_index_name='base_category_info_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'base_province': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='base_province_vector_index',
                keyword_index_name='base_province_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'base_subject_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='base_subject_info_vector_index',
                keyword_index_name='base_subject_info_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'cart_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='cart_info_vector_index',
                keyword_index_name='cart_info_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'chapter_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='chapter_info_vector_index',
                keyword_index_name='chapter_info_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'course_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='course_info_vector_index',
                keyword_index_name='course_info_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'knowledge_point': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='knowledge_point_vector_index',
                keyword_index_name='knowledge_point_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'order_detail': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='order_detail_vector_index',
                keyword_index_name='order_detail_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'test_paper': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='test_paper_vector_index',
                keyword_index_name='test_paper_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'test_question': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='test_question_info_vector_index',
                keyword_index_name='test_question_info_full_text_index',
                search_type=SearchType.HYBRID,
            ),
            'video_info': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='video_info_vector_index',
                keyword_index_name='video_info_full_text_index',
                search_type=SearchType.HYBRID,
            )
        }
        self.graph = Neo4jGraph(
            url=config.NEO4J_CONFIG["uri"],
            username=config.NEO4J_CONFIG['auth'][0],
            password=config.NEO4J_CONFIG['auth'][1]
        )
        self.json_parser = JsonOutputParser()
        self.str_parser = StrOutputParser()

    def _generate_cypher(self, question):
        prompt = """
                            你是一个专业的Neo4j Cypher查询生成器。你的任务是根据用户问题生成一条Cypher查询语句，用于从知识图谱中获取回答用户问题所需的信息。

                    用户问题：{question}
                    知识图谱结构信息：{schema_info}

                    要求：
                    1. 生成参数化Cypher查询语句，用param_0, param_1等代替具体值
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
        prompt = prompt.format(question=question, schema_info=self.graph.schema)###############################################################

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
        print('cypher:',cypher)
        print('entities_to_align:',entities_to_align)
        # 通过混合检索去做实体对齐
        aligned_entities = self._entity_align(entities_to_align)
        print('aligned_entities:',aligned_entities)
        # 执行Cypher语句
        query_result = self._execute_query(cypher, aligned_entities)
        print('query_result:',query_result)
        # 根据用户问题和查询结果生成答案
        answer = self._generate_answer(question, query_result)
        print('answer:',answer)
        return answer

    def _execute_query(self, cypher, aligned_entities):
        params = {aligned_entity['param_name']: aligned_entity['entity'] for aligned_entity in aligned_entities}
        return self.graph.query(cypher, params=params)

    def _generate_answer(self, question, query_result):
        prompt = """
        你是一个互联网领域在线教育智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = prompt.format(question=question, query_result=query_result)
        output = self.llm.invoke(prompt)
        return self.str_parser.invoke(output)


if __name__ == '__main__':
    chat_service = ChatService()
    chat_service.chat("java后端有什么要学的？")
