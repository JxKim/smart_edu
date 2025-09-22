from configuration import config
from datasync.utils import MysqlReader, Neo4jWriter
from uie_pytorch.uie_predictor import UIEPredictor


class ChapterInfoSync:
    def __init__(self):
        self.mysql_reader = MysqlReader()
        self.neo4j_writer = Neo4jWriter()
        self.uie = UIEPredictor(model='uie-base',
                                task_path= str(config.ROOT_DIR /'src'/'uie_pytorch'/'checkpoint'/'chapter'/'model_best'),
                                schema=['KN'],
                                device='gpu',
                                batch_size=128)

    def sync_KN(self):
        offset = 0
        while True:
            sql = """
                select 
                    id ,
                    chapter_name
                from chapter_info
                LIMIT %s OFFSET %s
                # limit 5
            """
            data = self.mysql_reader.read(sql,(128, offset))
            # data = self.mysql_reader.read(sql)

            if not data:
                 break

            ids = [item['id'] for item in data]
            chapter = [item['chapter_name'] for item in data]
            uie_chapter_list = self.uie(chapter)

            tag_properties =  []
            relations =  []
            for id, chapter_name in zip(ids, uie_chapter_list):
                if not chapter_name:
                    continue
                chapter_list = self._tiqu_text(chapter_name)
                for index, chapter_text in enumerate(chapter_list):
                    tag_id = '-'.join([str(id), str(index)])
                    property = {'id': tag_id, 'name': chapter_text}
                    tag_properties.append(property)
                    relations.append({'start_id': id, 'end_id': tag_id})

            # print(tag_properties)
            # print( relations)
            self.neo4j_writer.writer_nodes('Knowledge', tag_properties)
            self.neo4j_writer.writer_relations('HAVE', 'Chapter', 'Knowledge', relations)

            offset += 128
            print(offset)

    def _tiqu_text(self, chapter_name):
        result = []
        for item in chapter_name['KN']:
            result.append(item['text'])
        return  result



if __name__ == '__main__':
    ChapterInfoSync().sync_KN()


