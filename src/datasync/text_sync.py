import torch
from neo4j import GraphDatabase

from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter
from models.ner.predict import Predictor


class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extractor()
        self.driver = GraphDatabase.driver(**config.NEO4J_CONFIG)

    def _init_extractor(self):
        model = AutoModelForTokenClassification.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
        tokenizer = AutoTokenizer.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        return Predictor(model, tokenizer, device)

    def create_course_knowledge_relations(self):
        """
        在course_info和knowledge_point之间建立have关系
        通过course_info的id和知识点文本匹配knowledge_point的point_text
        若知识点不存在则自动创建新的knowledge_point节点
        """
        # 1. 获取课程信息和对应的知识点列表
        sql = """
              select id,
                     course_introduce
              from course_info
              """
        course_data = self.mysql_reader.read(sql)

        # 2. 提取每个课程的知识点
        relations = []
        for course in course_data:
            course_id = course['id']
            course_intro = course['course_introduce']

            # 提取知识点
            tags = self.extractor.extract([course_intro])[0]  # 假设返回单个课程的知识点列表
            print(f"课程ID: {course_id} 提取的知识点: {tags}")

            # 为每个知识点创建关系数据
            for tag in tags:
                # 构建关系数据，包含课程ID和知识点文本
                relations.append({
                    'start_id': course_id,  # 课程ID
                    'end_point_text': tag  # 知识点文本，用于匹配knowledge_point
                })

        if not relations:
            print("没有提取到任何知识点，无需创建关系")
            return

        # 3. 构建Cypher语句，先匹配知识点，不存在则创建，然后建立关系
        # 使用MERGE确保知识点存在，然后创建关系
        cypher = """
                UNWIND $batch AS item
                MATCH (course:course_info {id: item.start_id})
                // 匹配或创建知识点节点
                MERGE (knowledge:knowledge_point {point_text: item.end_point_text})
                // 确保关系存在
                MERGE (course)-[:have]->(knowledge)
            """

        # 4. 执行Cypher语句
        result = self.driver.execute_query(cypher, batch=relations)

        # 5. 打印执行结果
        counters = result.summary.counters
        print(f"课程-知识点关系创建结果: {counters}")
        print(f"创建的知识点节点数: {counters.nodes_created}")
        print(f"创建的关系数: {counters.relationships_created}")

        # 检查是否有知识点被创建（说明之前不存在）
        if counters.nodes_created > 0:
            print(f"注意: 有 {counters.nodes_created} 个知识点不存在，已自动创建")
        elif counters.relationships_created == 0:
            print("警告: 未创建任何course_info-have-knowledge_point关系，可能课程节点不存在")


if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    synchronizer.create_course_knowledge_relations()
