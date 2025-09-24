import os

from dotenv import load_dotenv
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph, Neo4jVector
from langchain_neo4j.vectorstores.neo4j_vector import SearchType
from langchain_openai import ChatOpenAI

load_dotenv()
api_key = os.getenv('OPENAI_API_KEY')
api_url = os.getenv('OPENAI_BASE_URL')
neo4j_nri = os.getenv('NEO4J_NRI')
neo4j_user = os.getenv('NEO4J_USER')
neo4j_password = os.getenv('NEO4J_PASSWORD')

class ChatService:
    def __init__(self):
        self.llm = ChatOpenAI(
            model = 'qwen-plus',
            api_key=api_key,
            base_url=api_url,
        )
        self.embedding_model = HuggingFaceEmbeddings(
            model_name='BAAI/bge-base-zh-v1.5',
            encode_kwargs={"normalize_embeddings": True}
        )
        self.json_paser = JsonOutputParser()
        self.str_paser = StrOutputParser()
        self.graph = Neo4jGraph(
            url=neo4j_nri,
            username=neo4j_user,
            password=neo4j_password
        )
        self.neo4j_vectors = {
            'Category':Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='category_vector_index',
                keyword_index_name='category_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'Course': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='course_vector_index',
                keyword_index_name='course_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'Subject': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='subject_vector_index',
                keyword_index_name='subject_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'KnowledgePoint': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='knowledge_point_vector_index',
                keyword_index_name='knowledge_point_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'ExamPaper': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='paper_vector_index',
                keyword_index_name='paper_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'Question': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='question_vector_index',
                keyword_index_name='question_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'Teacher': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='teacher_vector_index',
                keyword_index_name='teacher_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'User': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='user_vector_index',
                keyword_index_name='user_full_text_index',
                search_type=SearchType.HYBRID
            ),
            'Chapter': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=neo4j_nri,
                username=neo4j_user,
                password=neo4j_password,
                index_name='chapter_vector_index',
                keyword_index_name='chapter_full_text_index',
                search_type=SearchType.HYBRID
            ),
        }
    def chat(self, question):
        result = self._generate_cypher(question)
        cypher = result['cypher_query']
        initial_entities = result['entities_to_align']
        aligned_entities = self._entity_align(initial_entities)

        cypher_result = self._query_cypher(cypher, aligned_entities)

        answer = self._generate_answer(question, cypher_result)
        return  answer

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
        prompt = prompt.format(question=question, schema_info=self.graph.schema)
        output = self.llm.invoke(prompt)
        return self.json_paser.invoke(output)
    def _entity_align(self, initial_entities):
        for index, initial_entity in enumerate(initial_entities):
            label = initial_entity['label']
            entity = initial_entity['entity']
            aligned_entity = self.neo4j_vectors[label].similarity_search(entity, k=1)[0].page_content
            initial_entities[index]['entity'] = aligned_entity
        return initial_entities

    def _query_cypher(self, cypher, aligned_entities):
        params = {aligned_entity['param_name']:aligned_entity['entity'] for aligned_entity in aligned_entities}
        return self.graph.query(cypher, params= params)
    def _generate_answer(self, question, cypher_result):
        prompt = """
                你是一个教育培训智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                        用户问题: {question}
                        数据库返回结果: {query_result}
                """
        prompt = prompt.format(question=question, query_result=cypher_result)

        output = self.llm.invoke(prompt)
        return self.str_paser.invoke(output)
if __name__ == '__main__':
    service = ChatService()
    print(service.chat('查询所有老师'))