from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_deepseek import ChatDeepSeek
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jVector, Neo4jGraph
from langchain_neo4j.vectorstores.neo4j_vector import SearchType
from configuration.config import *
import dotenv
dotenv.load_dotenv()

class ChatService:
    def __init__(self):
        self.llm = ChatDeepSeek(model='deepseek-chat')
        self.embed_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                 encode_kwargs={'normalize_embeddings': True})

        self.neo4j_vectors = {
            'Category': Neo4jVector.from_existing_index(
                self.embed_model,
                url=NEO4J_CONFIG['uri'],
                username=NEO4J_CONFIG['auth'][0],
                password=NEO4J_CONFIG['auth'][1],
                index_name='category_vectorIndex',
                keyword_index_name='category_fullTextIndex',
                search_type=SearchType.HYBRID
            ),
            'Subject': Neo4jVector.from_existing_index(
                self.embed_model,
                url=NEO4J_CONFIG['uri'],
                username=NEO4J_CONFIG['auth'][0],
                password=NEO4J_CONFIG['auth'][1],
                index_name='subject_vectorIndex',
                keyword_index_name='subject_fullTextIndex',
                search_type=SearchType.HYBRID
            ),
            'Course': Neo4jVector.from_existing_index(
                self.embed_model,
                url=NEO4J_CONFIG['uri'],
                username=NEO4J_CONFIG['auth'][0],
                password=NEO4J_CONFIG['auth'][1],
                index_name='course_vectorIndex',
                keyword_index_name='course_fullTextIndex',
                search_type=SearchType.HYBRID
            ),
            'Chapter': Neo4jVector.from_existing_index(
                self.embed_model,
                url=NEO4J_CONFIG['uri'],
                username=NEO4J_CONFIG['auth'][0],
                password=NEO4J_CONFIG['auth'][1],
                index_name='chapter_vectorIndex',
                keyword_index_name='chapter_fullTextIndex',
                search_type=SearchType.HYBRID
            )
        }
        self.graph = Neo4jGraph(url=NEO4J_CONFIG['uri'],
                                username=NEO4J_CONFIG['auth'][0],
                                password=NEO4J_CONFIG['auth'][1])
        self.jsonParser = JsonOutputParser()
        self.strParser = StrOutputParser()

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
        prompt = prompt.format(question=question, schema_info=self.graph.schema)

        output = self.llm.invoke(prompt)
        result = self.jsonParser.invoke(output)
        return result

    def _entity_align(self, entities_to_align):
        for index, entity_to_align in enumerate(entities_to_align):
            label = entity_to_align['label']
            entity = entity_to_align['entity']
            aligned_entity = self.neo4j_vectors[label].similarity_search(entity, k=1)[0].page_content
            entities_to_align[index]['entity'] = aligned_entity
        return entities_to_align

    def _execute_query(self, cypher, aligned_entities):
        params = {aligned_entity['param_name']: aligned_entity['entity'] for aligned_entity in aligned_entities}
        return self.graph.query(cypher, params)

    def _generate_answer(self, question, query_result):
        prompt = """
        你是一个电商智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = PromptTemplate.from_template(prompt)
        prompt = prompt.format(question=question, query_result=query_result)
        output = self.llm.invoke(prompt)
        return self.strParser.invoke(output)

    def chat(self, question):
        # 1
        result = self._generate_cypher(question)
        cypher = result['cypher_query']
        entities_to_align = result['entities_to_align']
        print(cypher)
        print(entities_to_align)
        print('-----------------------')
        # 2
        entities_aligned = self._entity_align(entities_to_align)
        print(entities_aligned)
        print('-----------------------')
        # 3
        query_result = self._execute_query(cypher, entities_aligned)
        print(query_result)
        print('-----------------------')
        answer = self._generate_answer(question, query_result)
        print(answer)
        return answer

if __name__ == '__main__':
    chatServie = ChatService()
    chatServie.chat('有哪些课程？')