from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_deepseek import ChatDeepSeek
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jVector, Neo4jGraph
from neo4j_graphrag.types import SearchType

from configuration import config


class ChatService:
    def __init__(self):
        self.llm = ChatDeepSeek ( model='deepseek-chat' , api_key=config.API_KEY )
        self.embedding_model = HuggingFaceEmbeddings ( model_name='BAAI/bge-base-zh-v1.5' ,
                                                       encode_kwargs={'normalize_embeddings' : True} )

        self.neo4j_vectors = {
            '分类': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='category_vector_index',
                keyword_index_name='category_full_text_index',
                search_type='hybrid',
            ),
            '学科': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='subject_vector_index',
                keyword_index_name='subject_full_text_index',
                search_type='hybrid',
            ),
            '课程': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='course_vector_index',
                keyword_index_name='course_full_text_index',
                search_type='hybrid',
            ),
            '教师': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='teacher_vector_index',
                keyword_index_name='teacher_full_text_index',
                search_type='hybrid',
            ),
            '章节': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='chapter_vector_index',
                keyword_index_name='chapter_full_text_index',
                search_type='hybrid',
            ),
            '视频': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='video_vector_index',
                keyword_index_name='video_full_text_index',
                search_type='hybrid',
            ),
            '试卷': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='paper_vector_index',
                keyword_index_name='paper_full_text_index',
                search_type='hybrid',
            ),
            '试题': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='question_vector_index',
                keyword_index_name='question_full_text_index',
                search_type='hybrid',
            ),
            '学生': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='student_vector_index',
                keyword_index_name='student_full_text_index',
                search_type='hybrid',
            ),
            '知识点': Neo4jVector.from_existing_index(
                self.embedding_model,
                url=config.NEO4J_CONFIG["uri"],
                username=config.NEO4J_CONFIG['auth'][0],
                password=config.NEO4J_CONFIG['auth'][1],
                index_name='knowledge_vector_index',
                keyword_index_name='knowledge_full_text_index',
                search_type='hybrid',
            )
        }
        self.graph = Neo4jGraph(
            url=config.NEO4J_CONFIG["uri"],
            username=config.NEO4J_CONFIG['auth'][0],
            password=config.NEO4J_CONFIG['auth'][1]
        )
        self.json_parser = JsonOutputParser ( )
        self.str_parser = StrOutputParser ( )

    def _generate_cypher(self, question):
        prompt = """
        你是一个专业的Neo4j Cypher查询生成器，专门用于教育领域知识图谱。你的任务是根据用户问题生成准确的Cypher查询语句。

        ## 知识图谱结构信息：
        {{
          "节点类型": [
            "分类 (Category) - 属性: category_id, name",
            "学科 (Subject) - 属性: subject_id, name, category_id", 
            "课程 (Course) - 属性: course_id, name, course_slogan, teacher, subject_id",
            "章节 (Chapter) - 属性: chapter_id, name, course_id, is_free",
            "视频 (Video) - 属性: video_id, name, chapter_id, during_sec",
            "试卷 (Paper) - 属性: paper_id, name, course_id",
            "试题 (Question) - 属性: question_id, name, question_txt",
            "学生 (Student) - 属性: uid, nick_name, gender, birthday",
            "知识点 (KnowledgePoint) - 属性: id, name, point_txt",
            "教师 (Teacher) - 属性: teacher_name",
            "价格 (Price) - 属性: price_id, origin_price, actual_price"
          ],
          "关系类型": [
            "(:学科)-[:BELONG]->(:分类)",
            "(:课程)-[:BELONG]->(:学科)",
            "(:章节)-[:BELONG]->(:课程)", 
            "(:视频)-[:BELONG]->(:章节)",
            "(:试卷)-[:BELONG]->(:课程)",
            "(:试题)-[:BELONG]->(:试卷)",
            "(:课程)-[:HAVE]->(:教师)",
            "(:课程)-[:HAVE]->(:价格)",
            "(:学生)-[:FAVOR]->(:课程)",
            "(:学生)-[:ANSWER]->(:试题)",
            "(:学生)-[:WATCH]->(:视频)",
            "(:课程)-[:HAVE]->(:知识点)",
            "(:章节)-[:HAVE]->(:知识点)", 
            "(:试题)-[:HAVE]->(:知识点)",
            "(:知识点)-[:NEED]->(:知识点) - 先修关系",
            "(:知识点)-[:BELONG]->(:知识点) - 包含关系",
            "(:知识点)-[:RELATED]->(:知识点) - 相关关系"
          ],
          "特殊关系属性": [
            "FAVOR关系: 时间",
            "ANSWER关系: 是否正确, 得分", 
            "WATCH关系: 进度, 最后观看时间",
            "知识点关系: relation_type, source, confidence"
          ]
        }}

        ## 用户问题：
        {question}

        ## 查询生成要求：
        1. **参数化查询**：使用 $param_0, $param_1 等参数占位符代替具体值
        2. **实体对齐**：识别查询中需要模糊匹配的实体名称
        3. **性能优化**：使用合适的索引，避免全图扫描
        4. **结果限制**：合理使用 LIMIT 子句防止返回过多数据
        5. **关系方向**：注意关系的方向性，确保查询逻辑正确

        ## 输出格式要求：
        必须严格使用以下JSON格式：

        {{
          "cypher_query": "生成的Cypher查询语句，使用参数化形式",
          "entities_to_align": [
            {{
              "param_name": "param_0",
              "entity": "用户问题中的原始实体名称",
              "label": "对应的节点标签",
              "search_property": "用于搜索的属性名，通常是name"
            }}
          ],
          "query_explanation": "简要说明查询逻辑和预期结果"
        }}

        ## 示例：
        用户问题："查找包含机器学习知识点的课程"
        {{
          "cypher_query": "MATCH (k:知识点) WHERE k.name CONTAINS $param_0 MATCH (k)<-[:HAVE]-(c:课程) RETURN c.name AS course_name, k.name AS knowledge_name LIMIT 10",
          "entities_to_align": [
            {{
              "param_name": "param_0", 
              "entity": "机器学习",
              "label": "知识点",
              "search_property": "name"
            }}
          ],
          "query_explanation": "先找到包含'机器学习'关键词的知识点，然后查找拥有这些知识点的课程"
        }}

        现在请根据用户问题生成Cypher查询：
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
        return self.graph.query(cypher, params=params)

    def _generate_answer(self, question, query_result):
        prompt = """
        你是一个教育系统客服，根据用户问题，以及数据库查询结果生成一段简洁、准确的自然语言回答。
                用户问题: {question}
                数据库返回结果: {query_result}
        """
        prompt = prompt.format(question=question, query_result=query_result)
        output = self.llm.invoke(prompt)
        return self.str_parser.invoke(output)


if __name__ == '__main__':
    chat_service = ChatService()
    chat_service.chat("机器学习怎么学？")
