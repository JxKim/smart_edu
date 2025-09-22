import dotenv
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j.vectorstores.neo4j_vector import SearchType
from langchain_openai import ChatOpenAI
from langchain_neo4j import Neo4jGraph, Neo4jVector

from configuration import config

dotenv.load_dotenv()


class ChatServer:
    def __init__(self):
        self.llm=ChatOpenAI(model_name='qwen3-max-preview')
        self.json_output_parser = JsonOutputParser()
        self.str_output_parser = StrOutputParser()
        self.graph=Neo4jGraph(url=config.NEO4J_CONFIG['uri'],
                   username=config.NEO4J_CONFIG['auth'][0],
                   password=config.NEO4J_CONFIG['auth'][1]
                   )
        self.embed_model = HuggingFaceEmbeddings(
                        model_name="BAAI/bge-base-zh-v1.5",
                        model_kwargs={'device': 'cuda'},
                        encode_kwargs={'normalize_embeddings': True},
                        )
        self.stores={
            'Trademark':Neo4jVector.from_existing_index(
                        self.embed_model,
                        url=config.NEO4J_CONFIG['uri'],
                        username=config.NEO4J_CONFIG['auth'][0],
                        password=config.NEO4J_CONFIG['auth'][1],
                        index_name='Trademark_v',
                        keyword_index_name='Trademark_f',
                        search_type=SearchType.HYBRID,
                        ),
            'SKU': Neo4jVector.from_existing_index(
                self.embed_model,
                url=config.NEO4J_CONFIG['uri'],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='SKU_v',
                keyword_index_name='SKU_f',
                search_type=SearchType.HYBRID,
            ),
            'SPU': Neo4jVector.from_existing_index(
                self.embed_model,
                url=config.NEO4J_CONFIG['uri'],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='SPU_v',
                keyword_index_name='SPU_f',
                search_type=SearchType.HYBRID,
            ),
            'Category1': Neo4jVector.from_existing_index(
                self.embed_model,
                url=config.NEO4J_CONFIG['uri'],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='Category1_v',
                keyword_index_name='Category1_f',
                search_type=SearchType.HYBRID,
            ),
            'Category2': Neo4jVector.from_existing_index(
                self.embed_model,
                url=config.NEO4J_CONFIG['uri'],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='Category2_v',
                keyword_index_name='Category2_f',
                search_type=SearchType.HYBRID,
            ),
            'Category3':Neo4jVector.from_existing_index(
                        self.embed_model,
                        url=config.NEO4J_CONFIG['uri'],
                        username=config.NEO4J_CONFIG['auth'][0],
                        password=config.NEO4J_CONFIG['auth'][1],
                        index_name='Category3_v',
                        keyword_index_name='Category3_f',
                        search_type=SearchType.HYBRID,
                        )
        }


    def chat(self,question):
        # question-llm给出带参数cypher语句，并给出参数模版-Neo4jVector更改为正确的参数-真实cypher语句-查库，llm回答

        template=PromptTemplate.from_template(template="""
                    你是一个专业的Neo4j Cypher查询生成器。你的任务是根据用户问题生成一条Cypher查询语句，用于从知识图谱中获取回答用户问题所需的信息。

                    用户问题：{question}
                    知识图谱结构信息：{schema_info}

                    要求：
                    1. 生成参数化Cypher查询语句，用param_0, param_1等代替具体值
                    2. 识别需要对齐的实体
                    3. 必须严格使用以下JSON格式输出结果
                    {{
                      "cypher": "生成的Cypher语句",
                      "parameter_list": [
                        {{
                          "param_name": "param_0",
                          "entity": "原始实体名称",,
                          "label": "节点类型"
                        }}
                      ]
                    }}
                """)
        chain=template|self.llm|self.json_output_parser
        # print(self.graph.schema)
        output=chain.invoke({'question':question,'schema_info':self.graph.schema})
        cypher=output['cypher']
        parameter_list=output['parameter_list']
        params={}
        for parameter  in parameter_list:
            params[parameter['param_name']]=self.stores[parameter['label']].similarity_search(parameter['entity'], k=1)[0].page_content

        # print(params)
        # print(cypher)
        query_res=self.graph.query(cypher,params=params)
        # print(query_res)
        return self._generate_answer(query_res,question)

    def _generate_answer(self,query_res,question):
        template=PromptTemplate.from_template(template="""
        你是一个电商智能客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_res}
        """)
        chain=template|self.llm|self.str_output_parser
        return chain.invoke({'query_res':query_res,'question':question})

if __name__ == '__main__':
    server=ChatServer()
    print(server.chat('请列出属于xiaomi的产品'))





