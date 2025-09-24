from langchain_neo4j import Neo4jGraph, Neo4jVector
from langchain_huggingface import HuggingFaceEmbeddings
from neo4j_graphrag.types import SearchType

from configurations import config
# 构建索引的类
class IndexUtil:
    def __init__(self):
        # neo4j的连接对象
        self.graph=Neo4jGraph(
            url=config.NEO4J_CONFIG['uri'],
            username=config.NEO4J_CONFIG["auth"][0],
            password=config.NEO4J_CONFIG["auth"][1],
        )
        # 嵌入模型
        self.embedding_model=HuggingFaceEmbeddings(model_name="BAAI/bge-base-zh-v1.5",
                                                   encode_kwargs={"normalize_embeddings":True})

    # 构建全文索引
    def create_full_text_index(self, index_name, label,prop):
        # 创建cypher语句
        cypher=f"""
            CREATE FULLTEXT INDEX {index_name} if not EXISTS 
            FOR (n:{label}) ON each [n.`{prop}`]
        """
        # 执行cypher(返回的是一个字典的列表),这里没有返回值，因为是创建的语句
        self.graph.query(cypher)
        print(f"创建{label}的全文索引成功!")

    # 构建向量索引
    def create_vector_index(self, index_name, label,embedding_dim):
        cypher=f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
                FOR (n:{label})
                ON n.embedding
                OPTIONS {{ 
                    indexConfig: 
                    {{
                        `vector.dimensions`: {embedding_dim},
                        `vector.similarity_function`: 'cosine'
                    }}
                }}
        """
        self.graph.query(cypher)
        print(f"创建{label}的向量索引成功!")

    # 把name属性向量化为向量
    # 这里不能批量的主要原因是，这里的字段是变长的,不是固定的
    def add_embedding(self,args_list:list,label):
        temp_str=args_list[1]
        # 查询某个类别的数据
        cypher=f"""
            match (n:{label}) return n.{temp_str} as {temp_str},id(n) as id 
        """
        # print(cypher)
        # 这是一个字典列表[{'chapter_name': 'day20_11复习_总结', 'id': 20864}]
        results=self.graph.query(cypher)
        # print(results)

        # 把除了id的所有字段都向量化
        id_list=[res["id"] for res in results]
        length_list=[]
        for arg in args_list:
            if arg == "id":
                continue
            else:
                arg_val=[r[arg] for r in results]
                # print(arg_val)
                # 返回值是一个嵌套列表[[],[]]
                name_embedding=self.embedding_model.embed_documents(arg_val)
                # print(name_embedding)

                zipped=zip(id_list,name_embedding)

                batch=[]
                for id,embedding in zipped:
                    item={
                        "id":id,
                        "embedding":embedding
                    }
                    batch.append(item)

                # 把向量化的结果直接返回给数据库
                cypher=f"""
                    UNWIND $batch AS item 
                    match (n) where id(n)=item.id 
                    set n.embedding=item.embedding
                """
                self.graph.query(cypher,params={"batch":batch})
                length_list.append(len(name_embedding[0]))
        return length_list

if __name__ == '__main__':
    embedding_model = HuggingFaceEmbeddings(model_name="BAAI/bge-base-zh-v1.5",
                                            encode_kwargs={"normalize_embeddings": True})
    index_util=IndexUtil()

    # 章节数据的向量化、全文索引、向量索引
    # embedding_dim=index_util.add_embedding(["id","chapter_name"],"chapter")[0]
    # index_util.create_full_text_index(index_name="chapter_full_text_index", label="chapter",prop="chapter_name")
    # index_util.create_vector_index(index_name="chapter_vector_index", label="chapter", embedding_dim=embedding_dim)


    # # 章节知识点数据的向量化、全文索引、向量索引
    # embedding_dim = index_util.add_embedding(["id", "point"], "chapter_point")[0]
    # index_util.create_full_text_index(index_name="chapter_point_full_text_index", label="chapter_point",prop="chapter_point")
    # index_util.create_vector_index(index_name="chapter_point_vector_index", label="chapter_point", embedding_dim=embedding_dim)
    #
    # # 章节进度的向量化、全文索引、向量索引(这里有点问题，先这样)
    # # embedding_dim = index_util.add_embedding(["id", "position_sec"], "chapter_progress")[0]
    # # index_util.create_full_text_index(index_name="chapter_position_sec_full_text_index", label="chapter_progress",prop="chapter_progress")
    # # index_util.create_vector_index(index_name="chapter_position_sec_vector_index", label="chapter_progress",
    # #                                embedding_dim=embedding_dim)
    #
    # # 课程的向量化、全文索引、向量索引
    # embedding_dim = index_util.add_embedding(["id", "course_introduce"], "course")[0]
    # index_util.create_full_text_index(index_name="course_introduce_full_text_index", label="course_introduce",prop="course_introduce")
    # index_util.create_vector_index(index_name="course_introduce_vector_index", label="course_introduce",
    #                                embedding_dim=embedding_dim)
    #
    #
    # # 课程知识点
    # embedding_dim = index_util.add_embedding(["id", "point"], "course_point")[0]
    # index_util.create_full_text_index(index_name="course_point_full_text_index", label="course_point",prop="course_point")
    # index_util.create_vector_index(index_name="course_point_vector_index", label="course_point",
    #                                embedding_dim=embedding_dim)
    #
    # # 问题对应的知识点
    # embedding_dim = index_util.add_embedding(["id", "point_txt"], "knowledge_point")[0]
    # index_util.create_full_text_index(index_name="point_txt_full_text_index", label="knowledge_point",prop="point_txt")
    # index_util.create_vector_index(index_name="point_txt_vector_index", label="knowledge_point",
    #                                embedding_dim=embedding_dim)
    #
    # # 试卷
    # embedding_dim = index_util.add_embedding(["id", "paper_title"], "paper")[0]
    # index_util.create_full_text_index(index_name="paper_title_full_text_index", label="paper",prop="paper_title")
    # index_util.create_vector_index(index_name="paper_title_vector_index", label="paper",
    #                                embedding_dim=embedding_dim)
    #
    # # 问题
    # embedding_dim = index_util.add_embedding(["id", "question_txt"], "question")[0]
    # index_util.create_full_text_index(index_name="question_txt_full_text_index", label="question",prop="question_txt")
    # index_util.create_vector_index(index_name="question_txt_vector_index", label="question", embedding_dim=embedding_dim)

    # 问题选项（这里没写进去）
    # embedding_dim = index_util.add_embedding(["id", "option_txt"], "question_option")[0]
    # index_util.create_full_text_index(index_name="option_txt_full_text_index", label="question_option",prop="option_txt")
    # index_util.create_vector_index(index_name="option_txt_vector_index", label="question_option",
    #                                embedding_dim=embedding_dim)

    # 学科
    embedding_dim = index_util.add_embedding(["id", "subject_name"], "subject")[0]
    index_util.create_full_text_index(index_name="subject_name_full_text_index", label="subject",prop="subject_name")
    index_util.create_vector_index(index_name="subject_name_vector_index", label="subject",
                                   embedding_dim=embedding_dim)

    # 用户
    embedding_dim = index_util.add_embedding(["id", "real_name"], "user")[0]
    index_util.create_full_text_index(index_name="real_name_full_text_index", label="user",prop="real_name")
    index_util.create_vector_index(index_name="real_name_vector_index", label="user", embedding_dim=embedding_dim)

    # 视频
    embedding_dim = index_util.add_embedding(["id", "video_name"], "video")[0]
    index_util.create_full_text_index(index_name="video_name_full_text_index", label="video",prop="video_name")
    index_util.create_vector_index(index_name="video_name_vector_index", label="video", embedding_dim=embedding_dim)

    # 视频知识点
    embedding_dim = index_util.add_embedding(["id", "point"], "video_point")[0]
    index_util.create_full_text_index(index_name="video_point_full_text_index", label="video_point",prop="video_point")
    index_util.create_vector_index(index_name="video_point_vector_index", label="video_point", embedding_dim=embedding_dim)

    #
    # 向量索引的名字
    index_name = "chapter_vector_index"
    # 全文索引的名字
    keyword_index_name = "chapter_full_text_index"

    store = Neo4jVector.from_existing_index(embedding=embedding_model,
                                            url=config.NEO4J_CONFIG["uri"],
                                            username=config.NEO4J_CONFIG["auth"][0],
                                            password=config.NEO4J_CONFIG["auth"][1],
                                            index_name=index_name,
                                            keyword_index_name=keyword_index_name,
                                            search_type=SearchType.HYBRID)
    print(store.similarity_search("Flume", k=3))

    print(store.similarity_search("Flume",k=3)[0].page_content)
