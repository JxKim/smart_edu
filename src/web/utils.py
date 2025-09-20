from langchain_huggingface import HuggingFaceEmbeddings
from langchain_neo4j import Neo4jGraph
from configuration.config import *

class BuildIndex:
    def __init__(self):
        self.graph = Neo4jGraph(url=NEO4J_CONFIG['uri'],
                                username=NEO4J_CONFIG['auth'][0],
                                password=NEO4J_CONFIG['auth'][1])

        self.embed_model = HuggingFaceEmbeddings(model_name='BAAI/bge-base-zh-v1.5',
                                                encode_kwargs={'normalize_embeddings': True})

    def build_fullTextIndex(self, indexName, label, property):
        cypher = f'''
            create fulltext index {indexName} if not exists
            for (n:{label}) on each [n.{property}]
        '''
        self.graph.query(cypher)

    def build_vectorIndex(self,indexName,label,sourecProperty,embedProperty):
        embed_dim = self._add_embedding(label, sourecProperty,embedProperty)
        cypher = f'''
            create vector index {indexName} if not exists
            for (n:{label})
            on n.{embedProperty}
            options {{indexConfig:{{
                  `vector.dimensions`: {embed_dim},
                  `vector.similarity_function`: 'cosine'
                }}
            }}
        '''
        self.graph.query(cypher)

    def _add_embedding(self, label, sourecProperty, embedProperty):
        cypher = f'''
            match (n:{label}) return n.{sourecProperty} as text,id(n) as id
        '''
        results = self.graph.query(cypher)
        docs = [result['text'] for result in results]
        embeds = self.embed_model.embed_documents(docs)

        embed_items = []
        for result, embed in zip(results, embeds):
            item = {
                'id': result['id'],
                'embed': embed
            }
            embed_items.append(item)

        cypher = f'''
            unwind $batch as items
            match (n:{label}) where id(n) = items.id
            set n.{embedProperty} = items.embed
        '''
        self.graph.query(cypher, params={'batch':embed_items})
        return len(embeds[0])

if __name__ == '__main__':
    buildIndex = BuildIndex()
    buildIndex.build_fullTextIndex('category_fullTextIndex','CategoryInfo','name')
    buildIndex.build_vectorIndex('category_vectorIndex','CategoryInfo','name','embedding')

    buildIndex.build_fullTextIndex('subject_fullTextIndex', 'SubjectInfo', 'name')
    buildIndex.build_vectorIndex('subject_vectorIndex', 'SubjectInfo', 'name', 'embedding')

    buildIndex.build_fullTextIndex('course_fullTextIndex', 'CourseInfo', 'name')
    buildIndex.build_vectorIndex('course_vectorIndex', 'CourseInfo', 'name', 'embedding')
    #
    buildIndex.build_fullTextIndex('chapter_fullTextIndex', 'ChapterInfo', 'name')
    buildIndex.build_vectorIndex('chapter_vectorIndex', 'ChapterInfo', 'name', 'embedding')
    #
    # buildIndex.build_fullTextIndex('category2_fullTextIndex', 'Category2', 'name')
    # buildIndex.build_vectorIndex('category2_vectorIndex', 'Category2', 'name', 'embedding')
    #
    # buildIndex.build_fullTextIndex('category3_fullTextIndex', 'Category3', 'name')
    # buildIndex.build_vectorIndex('category3_vectorIndex', 'Category3', 'name', 'embedding')

