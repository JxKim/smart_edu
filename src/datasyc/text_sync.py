from transformers import AutoModelForTokenClassification, AutoTokenizer
import torch
from configuration import config
from datasyc.utils import MysqlReader, Neo4jWriter
from models.ner.predict import Predictor
import re
from bs4 import BeautifulSoup


class TextSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.extractor = self._init_extractor()

    def _init_extractor(self):
        model = AutoModelForTokenClassification.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
        tokenizer = AutoTokenizer.from_pretrained(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        return Predictor(model, tokenizer, device)
    def sync_knowledge_course(self):
        sql = """
            select id , 
                   course_introduce      
            from course_info
        """
        course_desc = self.mysql_reader.read(sql)
        ids = [item['id'] for item in course_desc]
        descs = [item['course_introduce'] for item in course_desc]
        tags_list = self.extractor.extract(descs)
        tag_propreties = []
        relations = []
        for id,tags in zip(ids,tags_list):
            for index,tag in enumerate(tags):
                tag_id = '-'.join([str(id),str(index)])
                property = {'id':tag_id,"name":tag}
                tag_propreties.append(property)
                relation = {'start_id':id,'end_id':tag_id}
                relations.append(relation)
        self.neo4j_writer.write_nodes("知识点", tag_propreties)
        self.neo4j_writer.write_relations("Have",'课程','知识点', relations)
    def sync_knowledge_questison(self):
        # 每次处理的记录数
        batch_size = 64
        # 起始偏移量
        offset = 0

        while True:
            # 使用LIMIT和OFFSET进行分页查询
            sql = f"""
                    select id, 
                           question_txt     
                    from test_question_info
                    LIMIT {batch_size} OFFSET {offset}
                """
            question_desc = self.mysql_reader.read(sql)

            # 如果没有数据了，退出循环
            if not question_desc:
                break

            # 处理当前批次的数据
            for index, item in enumerate(question_desc):
                text = item['question_txt']
                soup = BeautifulSoup(text, "html.parser")
                text = soup.get_text()
                text = re.sub(r'\s+', ' ', text).strip()
                text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s，。！？；：""''（）【】、,.!?;:\'\"]+', '', text)
                question_desc[index]['text'] = text

            ids = [item['id'] for item in question_desc]
            descs = [item['question_txt'] for item in question_desc]

            # 提取标签
            tags_list = self.extractor.extract(descs)

            tag_propreties = []
            relations = []
            for id, tags in zip(ids, tags_list):
                for index, tag in enumerate(tags):
                    tag_id = '-'.join([str(id), str(index)])
                    property = {'id': tag_id, "name": tag}
                    tag_propreties.append(property)
                    relation = {'start_id': id, 'end_id': tag_id}
                    relations.append(relation)

            # 写入当前批次的数据到Neo4j
            self.neo4j_writer.write_nodes("知识点", tag_propreties)
            self.neo4j_writer.write_relations("Have", '试题', '知识点', relations)

            # 增加偏移量，准备处理下一批
            offset += batch_size
            print(f"已处理 {offset} 条数据")

    def sync_knowledge_chapter(self):
        # 每次处理的记录数
        batch_size = 64
        # 起始偏移量
        offset = 0

        while True:
            # 使用LIMIT和OFFSET进行分页查询
            sql = f"""
                    select id, 
                           chapter_name,
                           course_id pre    
                    from chapter_info
                    LIMIT {batch_size} OFFSET {offset}
                """
            chapter_desc = self.mysql_reader.read(sql)

            # 如果没有数据了，退出循环
            if not chapter_desc:
                break


            ids = [item['id'] for item in chapter_desc]
            descs = [item['chapter_name'] for item in chapter_desc]
            pres = [item['pre'] for item in chapter_desc]
            # 提取标签
            tags_list = self.extractor.extract(descs)

            tag_propreties = []
            relations = []
            for id, tags,pre in zip(ids, tags_list,pres):
                for index, tag in enumerate(tags):
                    tag_id = '-'.join([str(id), str(index)])
                    property = {'id': tag_id, "name": tag, 'pre': pre}
                    tag_propreties.append(property)
                    relation = {'start_id': id, 'end_id': tag_id}

                    relations.append(relation)

            # 写入当前批次的数据到Neo4j
            self.neo4j_writer.write_chapter_knowledge("知识点", tag_propreties)
            self.neo4j_writer.write_relations("Have", '章节', '知识点', relations)


            # 增加偏移量，准备处理下一批
            offset += batch_size
            print(f"已处理 {offset} 条数据")

    def sync_knowledge_points(self):
        self.neo4j_writer.write_knowledge_relations()



if __name__ == '__main__':
    synchronizer = TextSynchronizer()
    # synchronizer.sync_knowledge_course()
    # synchronizer.sync_knowledge_questison()
    synchronizer.sync_knowledge_points()