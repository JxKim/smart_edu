from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_neo4j import Neo4jGraph
from langchain_openai import OpenAI
import dotenv

from config import config


class ChatService:
    def __init__(self):
        dotenv.load_dotenv()

        self.llm=OpenAI()
        self.str_parser=StrOutputParser()
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG["uri"],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])


    def _generate_cypher(self,question):
        prompt="""
                    你是一个专业的Neo4j Cypher查询生成器。你的任务是根据用户问题生成一条Cypher查询语句，用于从知识图谱中获取回答用户问题所需的信息。

                    用户问题：{question}
                    知识图谱结构信息：{schema_info}
                    
                    要求：严格按照知识图谱中的关系，不要胡乱编造关系
                """
        template = PromptTemplate.from_template(prompt)
        prompt=template.format(question=question,schema_info=self.graph.schema)
        output=self.llm.invoke(prompt)
        result=self.str_parser.invoke(output)
        return result

    def chat(self,question):
        result=self._generate_cypher(question)
        print(result)
        result = self.graph.query(result)
        print(result)
        answer = self._generatr_answer(question,result)
        print(answer)

    def _generatr_answer(self,question,result):
        prompt="""
        你是一个电商智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = prompt.format(question=question, query_result=result)
        output = self.llm.invoke(prompt)
        return self.str_parser.invoke(output)


if __name__=="__main__":
    server=ChatService()
    # server.chat("查询编程技术分类下有哪些学科")
    server.chat("查询前端学科下(Subject标签)下有哪些课程(Course标签)")

