import os
import sys

import dotenv
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jVector, Neo4jGraph
from pathlib import Path
SRC_PATH=Path(__file__).parent.parent
sys.path.append(str(SRC_PATH))
from conf import config
dotenv.load_dotenv()
from langchain_neo4j import Neo4jGraph
from neo4j_graphrag.types import SearchType
from langchain_community.embeddings import DashScopeEmbeddings
# 下载模型到本地目录（比如 ./models/bge-base-zh-v1.5）
from huggingface_hub import snapshot_download

class IndexUtils:
    def __init__(self):

        self.graph = Neo4jGraph(url="neo4j://host.docker.internal:7687",
                                username="neo4j",
                                password="neo4jneo4j",
                                database="testdatabase")
        #获取嵌入模型
        self.embedding_model = HuggingFaceEmbeddings(model_name=str(config.CHECKPOINT_DIR/'bge-base-zh-v1.5'),
                                                     encode_kwargs={'normalize_embeddings': True})
        print('模型创建完成')

    def create_full_text_index(self,index_name,label,property_name):
        cypher=f'''
            create fulltext index {index_name}  if not exists
            for (n:{label}) on each[n.{property_name}]
        '''
        self.graph.query(cypher)
        print(f'全文索引创建完成')

    def create_vector_index(self,index_name,label,source_property,embedding_property):
        embedding_dim=self.add_embedding(label,source_property,embedding_property)
        cypher=f'''
            create VECTOR INDEX {index_name} if not exists
            FOR (m:{label})
            ON m.{embedding_property}
            OPTIONS {{indexConfig: 
            {{
            `vector.dimensions`: {embedding_dim},
            `vector.similarity_function`: 'cosine'}}
            }}
        '''
        self.graph.query(cypher)
        print(f'向量索引创建完成')

    def add_embedding(self, label, source_property, embedding_property):
        cypher = f'''
            MATCH (n:{label})
            return n.{source_property} as text, elementId(n) as id
        '''
        results = self.graph.query(cypher)
        docs = [str(doc).replace("\n", " ") if doc is not None else "" for doc in results]

        try:
            embeddings = self.embedding_model.embed_documents(docs)
        except KeyboardInterrupt:
            print("嵌入生成被用户中断")
            raise
        except Exception as e:
            print(f"嵌入生成失败: {e}")
            raise

        batch = [{'id': result['id'], 'embedding': embedding}
                 for result, embedding in zip(results, embeddings)]

        cypher = f'''
            UNWIND $batch as item
            MATCH (n:{label})
            WHERE elementId(n)=item.id
            SET n.{embedding_property}=item.embedding
        '''
        self.graph.query(cypher, {'batch': batch})
        print(f'向量嵌入添加完成')
        return len(embeddings[0])



    def neo4j_vector(self,index_name,keyword_index_name):
        return Neo4jVector.from_existing_index(
                self.embedding_model,
                url="neo4j://host.docker.internal:7687",
                # url="neo4j://127.0.0.1:7687",
                username="neo4j",
                password="neo4jneo4j",
                database="testdatabase",
                index_name=index_name,
                keyword_index_name=keyword_index_name,
                search_type=SearchType.HYBRID
                )



if __name__ == '__main__':
    index_utils=IndexUtils()
    # index_utils.create_full_text_index('chapter_full_index','chapter','name')
    # index_utils.create_vector_index('chapter_vector_index','chapter','name','embedding')
    #
    # index_utils.create_full_text_index('course_full_index','course','name')
    # index_utils.create_vector_index('course_vector_index','course','name','embedding')
    #
    # index_utils.create_full_text_index('teacher_full_index','teacher','name')
    # index_utils.create_vector_index('teacher_vector_index','teacher','name','embedding')
    #
    # index_utils.create_full_text_index('subject_full_index','subject','name')
    # index_utils.create_vector_index('subject_vector_index','subject','name','embedding')
    #
    # index_utils.create_full_text_index('ChapterKnowledgePoint_full_index','ChapterKnowledgePoint','name')
    # index_utils.create_vector_index('ChapterKnowledgePoint_vector_index','ChapterKnowledgePoint','name','embedding')

    #
    # index_utils.create_full_text_index('question_full_index','question','name')
    # index_utils.create_vector_index('question_vector_index','question','name','embedding')

    # index_utils.create_full_text_index('price_full_index','price','name')
    # index_utils.create_vector_index('price_vector_index','price','name','embedding')

    # index_utils.create_full_text_index('student_full_index','student','id')
    # index_utils.create_vector_index('student_vector_index','student','id','embedding')
    #
    # index_utils.create_full_text_index('teacher_full_index','teacher','name')
    # index_utils.create_vector_index('teacher_vector_index','teacher','name','embedding')
    #
    # index_utils.create_full_text_index('test_paper_full_index','test_paper','name')
    # index_utils.create_vector_index('test_paper_vector_index','test_paper','name','embedding')

    index_utils.create_full_text_index('video_full_index','video','name')
    index_utils.create_vector_index('video_vector_index','video','name','embedding')
