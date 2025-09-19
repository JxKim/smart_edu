from datasync.utils import MysqlReader, Neo4jWriter


class NodesSynchronizer:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()

    # 分类
    def synchronize_category(self):
        pass
    # 学科
    def synchronize_subject(self):
        pass

    # 课程
    def synchronize_course(self):
        pass
    # 教师
    def synchronize_teacher(self):
        pass
    # 价格
    def synchronize_price(self):
        pass

    # 章节
    def synchronize_chapter(self):
        pass
    # 视频
    def synchronize_video(self):
        pass
    # 试卷
    def synchronize_paper(self):
        pass
    # 试题
    def synchronize_question(self):
        pass

if __name__ == '__main__':
    nodes_synchronizer = NodesSynchronizer()