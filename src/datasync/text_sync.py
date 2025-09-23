import pymysql
from neo4j import GraphDatabase
from tqdm import tqdm

import predictor
from configurations import config


class Sync_Unstructured:
    def __init__(self):
        self.connection=pymysql.connect(**config.SQL_CONFIG)
        self.driver=GraphDatabase.driver(**config.NEO4J_CONFIG)

    # 写入节点的方法
    def write_node(self,id,res_list,label):
        # 如果只有一个元素,包装成列表
        if isinstance(res_list,str):
            res_list = [res_list]
        for res in res_list:
            cypher=f"""
                merge (n:{label} {{id:$id,point:$res}})
            """
            self.driver.execute_query(query_=cypher,parameters_={"id":id,"res":res})

    # 写入关系的方法
    def write_relation(self,start_label,end_label,id,type):
        cypher=f"""
            match (start:{start_label}{{id:$id}} ),(end:{end_label} {{id:$id}})
            merge (start)-[:{type}]->(end)
        """
        self.driver.execute_query(query_=cypher,parameters_={"id":id})

    # 写入视频数据
    def sync_video_point(self):
        # 在sql中读数据
        # language=sql
        sql="""
            select
            id,
            video_name as message
            from 
            video_info
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()

        # 提前存好所有的id
        ids = [res[0] for res in results]
        # 把读取好的数据直接清洗好
        results = [res[1].strip().split(".")[0] for res in results if len(res[1].strip().split(".")[0]) >= 3]

        # 把清洗好的数据直接给到预测
        final_list = []
        for index,res in enumerate(tqdm(results)):
            temp=predictor.predict_uie(res)
            if temp is None:
                continue
            final_list.append(temp[0]["知识点"][0]["text"])
            # if index==3:
            #     break
        # 数据结构是这样的[[{'知识点': [{'text': '尚硅谷,Flume'}]}], [{'知识点': [{'text': 'Flume'}]}]]
        print("视频数据文件标注完毕!")
        print(final_list)
        for id,temp in tqdm(zip(ids,final_list)):
            if "," in temp:
                res_list=temp.split(",")
                # 写入新节点
                self.write_node(id,res_list,"video_point")
            else:
                self.write_node(id, temp,"video_point")
        print("写入节点完成!")
        # 写入新关系
        for id,temp in tqdm(zip(ids,final_list)):
            self.write_relation("video","video_point",id,"HAVE_VIDEO_POINT")
        print("写入关系完成!")

    # 写入章节数据
    def sync_chapter_point(self):
        # 在sql中读数据
        # language=sql
        sql="""
            select
            id,
            chapter_name as message
            from 
            chapter_info
        """

        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()

        # 提前存好所有的id
        ids = [res[0] for res in results]
        # 把读取好的数据直接清洗好
        results = [res[1].strip().split(".")[0] for res in results if len(res[1].strip().split(".")[0]) >= 3]

        # 把清洗好的数据直接给到预测
        final_list = []
        for index,res in enumerate(tqdm(results)):
            temp=predictor.predict_uie(res)
            if temp is None:
                continue
            final_list.append(temp[0]["知识点"][0]["text"])
            # if index==3:
            #     break
        # 数据结构是这样的[[{'知识点': [{'text': '尚硅谷,Flume'}]}], [{'知识点': [{'text': 'Flume'}]}]]
        print("章节数据文件标注完毕!")
        print(final_list)
        for id,temp in tqdm(zip(ids,final_list)):
            if "," in temp:
                res_list=temp.split(",")
                # 写入新节点
                self.write_node(id,res_list,"chapter_point")
            else:
                self.write_node(id, temp,"chapter_point")
        print("写入章节节点完成!")
        # 写入新关系
        for id,temp in tqdm(zip(ids,final_list)):
            self.write_relation("chapter","chapter_point",id,"HAVE_CHAPTER_POINT")
        print("写入章节关系完成!")

    # 写入课程数据
    def sync_course_point(self):
        # 在sql中读数据
        # language=sql
        sql="""
            select
            id,
            course_introduce as message
            from 
            course_info
        """

        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()

        # 提前存好所有的id
        ids = [res[0] for res in results]
        # 把读取好的数据直接清洗好
        results = [res[1].strip().split(".")[0] for res in results if len(res[1].strip().split(".")[0]) >= 3]

        # 把清洗好的数据直接给到预测
        final_list = []
        for index,res in enumerate(tqdm(results)):
            temp=predictor.predict_uie(res)
            if temp is None:
                continue
            final_list.append(temp[0]["知识点"][0]["text"])
            # if index==3:
            #     break
        # 数据结构是这样的[[{'知识点': [{'text': '尚硅谷,Flume'}]}], [{'知识点': [{'text': 'Flume'}]}]]
        print("课程数据文件标注完毕!")
        print(final_list)
        for id,temp in tqdm(zip(ids,final_list)):
            if "," in temp:
                res_list=temp.split(",")
                # 写入新节点
                self.write_node(id,res_list,"course_point")
            else:
                self.write_node(id, temp,"course_point")
        print("写入课程节点完成!")
        # 写入新关系
        for id,temp in tqdm(zip(ids,final_list)):
            self.write_relation("course","course_point",id,"HAVE_COURSE_POINT")
        print("写入课程关系完成!")

if __name__=="__main__":
    sync_un=Sync_Unstructured()
    # 写入视频节点数据
    # sync_un.sync_video_point()
    # 写入章节的节点数据
    # sync_un.sync_chapter_point()
    # 写入课程介绍
    sync_un.sync_course_point()






