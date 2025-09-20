from datasync.utils import MysqlReader, Neo4jWriter


class TableSynchronizer:
    def __init__(self):
        self.reader = MysqlReader()
        self.writer = Neo4jWriter()

    def sync_category(self):
        sql = """
        select id,category_name name
        from base_category_info"""
        properties = self.reader.read(sql)
        self.writer.write_nodes("category", properties)




if __name__ == '__main__':
    synchronizer = TableSynchronizer()

    synchronizer.sync_category()
