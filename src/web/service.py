import os
import dotenv

dotenv.load_dotenv()
api_key=os.getenv("DASHSCOPE_API_KEY")
from langchain_community.llms import Tongyi
from langchain_openai import ChatOpenAI
from web.index_utils import IndexUtils
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_neo4j import Neo4jVector, Neo4jGraph
from neo4j_graphrag.types import SearchType


class ChatService:
    def __init__(self):
        self.index_util=IndexUtils()
        self.llm = Tongyi(model_name='deepseek-v3.1', api_key=os.getenv("DASHSCOPE_API_KEY"))
        self.graph=self.index_util.graph
        self.neo4j_vectors={
            'chapter':self.index_util.neo4j_vector("chapter_vector_index",'chapter_full_index'),
            'course':self.index_util.neo4j_vector("course_vector_index",'course_full_index'),
            'ChapterKnowledgePoint':self.index_util.neo4j_vector("ChapterKnowledgePoint_vector_index",'ChapterKnowledgePoint_full_index'),
            'teacher':self.index_util.neo4j_vector("teacher_vector_index",'teacher_full_index'),
            'subject':self.index_util.neo4j_vector("subject_vector_index",'subject_full_index'),
            'question':self.index_util.neo4j_vector("question_vector_index",'question_full_index'),
            'price':self.index_util.neo4j_vector("price_vector_index",'price_full_index'),
            'student':self.index_util.neo4j_vector("student_vector_index",'student_full_index'),
            'test_paper':self.index_util.neo4j_vector("test_paper_vector_index",'test_paper_full_index'),
            'video':self.index_util.neo4j_vector('video_vector_index','video_full_index')

        }

        self.json_parser=JsonOutputParser()
        self.str_parser=StrOutputParser()

    def _generate_cypher(self,question):
        prompt = """
                你是一个专业的 Neo4j Cypher 查询生成器。你的唯一任务是根据用户问题和知识图谱结构，生成一条**参数化**的 Cypher 查询语句，并识别需要对齐的实体。

                ### 输入信息：
                - 用户问题：{question}
                - 知识图谱结构信息：{schema_info}

                ### 输出要求：
                1. 生成的 Cypher 查询必须使用参数占位符（如 `param_0`, `param_1`, ...），**禁止硬编码具体值**。
                2. 识别所有需要实体对齐的节点，并在 `entities_to_align` 中列出：
                   - `param_name`：查询中使用的参数名（如 `"param_0"`）
                   - `entity`：用户问题中提到的原始实体名称（字符串）
                   - `label`：该实体在图谱中应匹配的节点标签（类型）
                3. 输出必须为**严格合法的 JSON 格式**，且仅包含以下两个字段：
                   - `"cypher_query"`：字符串，生成的 Cypher 语句
                   - `"entities_to_align"`：列表，每个元素为包含上述三个字段的对象

                ### 示例格式（非真实输出，仅展示结构）：
                {{
                  "cypher_query": "MATCH (n:Person {{name: $param_0}})-[:WORKS_AT]->(c:Company) RETURN c.name",
                  "entities_to_align": [
                    {{
                      "param_name": "param_0",
                      "entity": "张三",
                      "label": "Person"
                    }}
                  ]
                }}

                ### 重要提醒：
                - 不要解释、不要额外文本、不要 markdown。
                - 仅输出一个 JSON 对象。
                - 确保 Cypher 语法正确，使用 `$param_x` 语法绑定参数。
                - 如果无实体需对齐，`entities_to_align` 应为空列表 `[]`。

                现在，请根据以下输入生成结果：
                """
        prompt=PromptTemplate.from_template(prompt)
        prompt=prompt.format(question=question,schema_info=self.graph.schema)
        output=self.llm.invoke(prompt)
        return self.json_parser.parse(output)

    #实体对齐，将用户输入的实体通过混合检索
    def _entity_align(self,entities_to_align):
        for index,entity_to_align in enumerate(entities_to_align):
            label=entity_to_align['label']
            entity=entity_to_align['entity']
            search_result = self.neo4j_vectors[label].similarity_search(entity, k=1)[0]
            page_content = search_result.page_content
            # if isinstance(page_content, list):
            #     page_content = page_content[0]  # 提取第一个元素作为字符串
            # aligned_entity = page_content

            entities_to_align[index]['entity']=page_content
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

        print('数据库查询结果',query_result)
        # 根据用户问题和查询结果生成答案
        answer = self._generate_answer(question, query_result)
        print(answer)
        return answer
    #执行Cypher查询语句,查询实体名称
    def _execute_query(self, cypher, aligned_entities):
        params = {aligned_entity['param_name']: aligned_entity['entity'] for aligned_entity in aligned_entities}
        return self.graph.query(cypher, params=params)

    def _generate_answer(self, question, query_result):
        prompt = """
        你是一个在线教育智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答，如果数据库查询结果为空，请自行总结知识点并输出。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = prompt.format(question=question, query_result=query_result)
        output = self.llm.invoke(prompt)
        return self.str_parser.invoke(output)


if __name__ == '__main__':
    chat_service = ChatService()
    chat_service.chat("包含java知识点的课程有哪些")

