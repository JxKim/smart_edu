# 先提前加载配置文件
from dotenv import load_dotenv
load_dotenv()

from langchain_huggingface import HuggingFaceEmbeddings
from neo4j_graphrag.types import SearchType

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_neo4j import Neo4jGraph, Neo4jVector
from langchain_openai import ChatOpenAI

from configurations import config

class ChatService:
    def __init__(self):
        self.llm = ChatOpenAI(model="gpt-4o-mini")
        # 操作图数据库的对象
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG['uri'], username=config.NEO4J_CONFIG["auth"][0],
                                password=config.NEO4J_CONFIG["auth"][1])
        # json解析器
        self.json_parser = JsonOutputParser()
        self.embedding_model = HuggingFaceEmbeddings(model_name="BAAI/bge-base-zh-v1.5",
                                                     encode_kwargs={"normalize_embeddings": True})

        self.neo4j_vectors = {
            # 章节的向量索引
            'chapter': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='chapter_vector_index', keyword_index_name='chapter_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 章节知识点的向量索引
            'chapter_point': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='chapter_point_vector_index', keyword_index_name='chapter_point_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 课程介绍的向量索引
            'course_introduce': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='course_introduce_vector_index', keyword_index_name='course_introduce_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 课程知识点的向量索引
            'course_point': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='course_point_vector_index', keyword_index_name='course_point_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 问题对应知识点的向量索引
            'point_txt': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='point_txt_vector_index', keyword_index_name='point_txt_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 问题对应的向量索引
            'question_txt': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0], password=config.NEO4J_CONFIG['auth'][1],
                index_name='question_txt_vector_index', keyword_index_name='question_txt_full_text_index',
                search_type=SearchType.HYBRID, ),
            # 真实姓名对应的向量索引
            'real_name': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                                                         username=config.NEO4J_CONFIG['auth'][0],
                                                         password=config.NEO4J_CONFIG['auth'][1],
                                                         index_name='real_name_vector_index',
                                                         keyword_index_name='real_name_full_text_index',
                                                         search_type=SearchType.HYBRID, ),
            # 学科名字对应的向量索引
            'subject_name': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                                                         username=config.NEO4J_CONFIG['auth'][0],
                                                         password=config.NEO4J_CONFIG['auth'][1],
                                                         index_name='subject_name_vector_index',
                                                         keyword_index_name='subject_name_full_text_index',
                                                         search_type=SearchType.HYBRID, ),
            'video': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                                                            username=config.NEO4J_CONFIG['auth'][0],
                                                            password=config.NEO4J_CONFIG['auth'][1],
                                                            index_name='video_name_vector_index',
                                                            keyword_index_name="video_name_full_text_index",
                                                            search_type=SearchType.HYBRID, ),

            'video_point': Neo4jVector.from_existing_index(self.embedding_model, url=config.NEO4J_CONFIG["uri"],
                                                            username=config.NEO4J_CONFIG['auth'][0],
                                                            password=config.NEO4J_CONFIG['auth'][1],
                                                            index_name='video_point_vector_index',
                                                            keyword_index_name='video_point_full_text_index',
                                                            search_type=SearchType.HYBRID, )
        }

    # 实体抽取和生成cypher语句,可以考虑分开为两步
    def generate_cypher_entity(self, question):
        # 拿到图数据库的基础信息
        schema = self.graph.schema
        template = """
                    你是一个专业的Neo4j Cypher查询生成器。你的任务是根据用户问题生成一条Cypher查询语句，用于从知识图谱中获取回答用户问题所需的信息。

                            用户问题：{question}
                            知识图谱结构信息：{schema_info}

                            要求：
                            1. 生成参数化Cypher查询语句，用$param_0, $param_1等代替具体值
                            2. 识别需要对齐的实体
                            3. 必须严格使用以下JSON格式输出结果
                            {{
                              "cypher_query": "生成的Cypher语句",
                              "entities_to_align": [
                                {{
                                  "param_name": "param_0",
                                  "entity": "原始实体名称",
                                  "label": "节点类型",
                                }}
                              ]
                    }}
                """
        # 提示词模版
        prompt_template = PromptTemplate.from_template(template)
        # 链式调用（模版-模型-输出解析器）
        chain = prompt_template | self.llm | self.json_parser
        res = chain.invoke(input={"question": question, "schema_info": schema})
        print("实体抽取拿到的结果是:", res)
        return res

    # 实体对齐(查询图数据库)
    def entity_alignment(self, origin_entities):
        # 原始的实体和标签,创建一个向量搜索的工具，用这个工具去做实体对齐(不用给每一个类别都写一个工具)
        for index, entities in enumerate(origin_entities):
            label = entities["label"]
            entity = entities["entity"]
            print(entities)
            align_utils = self.neo4j_vectors[label].similarity_search(entity, k=1)[0].page_content
            completion_entity = align_utils.similarity_search(entity, k=1)

            origin_entities[index]["entity"] = completion_entity
        print("实体对齐之后的结果是：", origin_entities)
        return origin_entities

    # 生成最终的cypher语句
    def generate_final_cypher(self, origin_cypher_entities, complete_entities):
        origin_cypher = origin_cypher_entities["cypher_query"]
        # 这里最后期望拿到的结构是{{param_0:“苹果”},{param_1:“小米”}}
        params_dict = {}
        for index, item in enumerate(complete_entities):
            param_name = item["param_name"]
            entity = item["entity"][index].page_content
            params_dict[param_name] = entity

        res = self.graph.query(origin_cypher, params=params_dict)
        print("最终cypher语句是:",res)
        return res

    # 生成最终的回答
    def get_answer(self, question,query_result):
        template = """
                你是一个在线教育系统的客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                        用户问题: {question}
                        数据库返回结果: {query_result}
                """
        prompt_template = PromptTemplate.from_template(template)

        chain = prompt_template | self.llm

        res=chain.invoke(input={"question": question, "query_result": query_result})
        print("最终到大模型查询的结果是",res)
        return res

    def chat(self, question):
        # 1. 生成cypher语句;生成需要对其的实体
        origin_cypher_entities = self.generate_cypher_entity(question=question)
        # 2. 实体对齐
        complete_entities = self.entity_alignment(origin_entities=origin_cypher_entities["entities_to_align"])
        # 3. 生成真正的cypher语句：把对齐后的实体,给到之前的cypher语句
        results_list = self.generate_final_cypher(origin_cypher_entities, complete_entities)
        # 4.调用大模型,让模型组织成自然语言
        final_result=self.get_answer(question=question,query_result=results_list)
        return final_result
if __name__ == "__main__":
    chat_service = ChatService()
    chat_service.chat("HTML+CSS基础有哪些知识点")
