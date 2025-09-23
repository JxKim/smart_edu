import dotenv
dotenv.load_dotenv()
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.graphs import Neo4jGraph

from configuration import config

#https://python.langchain.com/docs/tutorials/graph/
class IndexUtil:
    def __init__(self):
        self.graph = Neo4jGraph(url=config.NEO4J_CONFIG['uri'],
                                username=config.NEO4J_CONFIG['auth'][0],
                                password=config.NEO4J_CONFIG['auth'][1])

        #https://python.langchain.com/api_reference/huggingface/embeddings/langchain_huggingface.embeddings.huggingface.HuggingFaceEmbeddings.html#langchain_huggingface.embeddings.huggingface.HuggingFaceEmbeddings
        #词嵌入模型 https://huggingface.co/BAAI/bge-small-zh-v1.5
        self.embedding_model = HuggingFaceEmbeddings(model_name = 'BAAI/bge-small-zh-v1.5',
                                                     encode_kwargs = {'normalize_embeddings': True})


    #https://neo4j.ac.cn/docs/cypher-manual/current/indexes/semantic-indexes/full-text-indexes/
    def create_full_text_index(self,index_name,label,property):
        cypher = f'''
        CREATE FULLTEXT INDEX {index_name}  IF NOT EXISTS
        FOR (n:{label}) ON EACH [n.{property}]
        '''
        self.graph.query(cypher) #执行cypher语句
        print(f'🍉{label}全文索引cypher语句-->',cypher)

    #https://neo4j.ac.cn/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/#create-vector-index
    def create_vector_index(self,index_name,label,source_property,embedding_property):
        embedding_dim = self._add_embedding(label,source_property,embedding_property)
        cypher = f'''
        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
        FOR (m:{label})
        ON m.{embedding_property}
        OPTIONS {{ indexConfig: {{
         `vector.dimensions`: {embedding_dim},
         `vector.similarity_function`: 'cosine'
        }}
        }}
        '''
        self.graph.query(cypher)
        print(f'🐦{label}向量索引cypher语句-->', cypher)

    # https://python.langchain.com/docs/integrations/providers/huggingface/#huggingfaceembeddings
    # https://python.langchain.com/docs/integrations/text_embedding/huggingfacehub/
    # https://python.langchain.com/api_reference/huggingface/embeddings/langchain_huggingface.embeddings.huggingface.HuggingFaceEmbeddings.html#langchain_huggingface.embeddings.huggingface.HuggingFaceEmbeddings
    def _add_embedding(self,label,source_property,embedding_property):
        '''
        1.从图数据库中提取指定节点的文本数据。
        2.使用嵌入模型将文本数据转换为嵌入向量。
        3.将嵌入向量存储回图数据库中对应的节点属性中。
        4.返回嵌入向量的维度。
        '''
        cypher = f'''
                 MATCH (n:{label}) RETURN n.{source_property} AS text, id(n) AS id
        '''
        results = self.graph.query(cypher) #[{'text': '编程技术', 'id': 15494}]
        docs = [result['text'] for result in results]
        embeddings = self.embedding_model.embed_documents(docs)

        batch_embedding = []
        for result,embedding in zip(results,embeddings):
            item = {'id':result['id'],'embedding':embedding}
            batch_embedding.append(item)

        #给某标签类型的节点设置embedding属性
        cypher = f'''
            UNWIND $BATCH_EMBEDDING AS item
            MATCH (n:{label}) WHERE id(n) = item.id
            SET n.{embedding_property} = item.embedding
        '''
        self.graph.query(cypher,params={'BATCH_EMBEDDING':batch_embedding})
        return len(embeddings[0])




if __name__ == '__main__':
    index_util = IndexUtil()

    #1.课程分类节点--索引
    index_util.create_full_text_index('coursecategory_full_text_index','CourseCategory','name')
    index_util.create_vector_index('coursecategory_vector_index','CourseCategory','name','embedding')
    #2.学科节点--索引
    index_util.create_full_text_index('subject_full_text_index', 'Subject', 'name')
    index_util.create_vector_index('subject_vector_index', 'Subject', 'name', 'embedding')
    #3.课程节点--索引
    index_util.create_full_text_index('course_full_text_index', 'Course', 'name')
    index_util.create_vector_index('course_vector_index', 'Course', 'name', 'embedding')
    #4.章节节点--索引
    index_util.create_full_text_index('chapter_full_text_index', 'Chapter', 'chapter_name')
    index_util.create_vector_index('chapter_vector_index', 'Chapter', 'chapter_name', 'embedding')
    #5.试卷节点--索引
    index_util.create_full_text_index('paper_full_text_index', 'Paper', 'paper_title')
    index_util.create_vector_index('paper_vector_index', 'Paper', 'paper_title', 'embedding')
    #6.试题节点--索引
    index_util.create_full_text_index('testquestion_full_text_index', 'TestQuestion', 'question_txt')
    index_util.create_vector_index('testquestion_vector_index', 'TestQuestion', 'question_txt', 'embedding')
    #7.学生节点--索引
    index_util.create_full_text_index('student_full_text_index', 'Student', 'real_name')
    index_util.create_vector_index('student_vector_index', 'Student', 'real_name', 'embedding')
    #8.视频节点--索引
    index_util.create_full_text_index('video_full_text_index', 'Video', 'video_name')
    index_util.create_vector_index('video_vector_index', 'Video', 'video_name', 'embedding')
    print('----work done！！-----')