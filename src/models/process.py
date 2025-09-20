from os import truncate

from datasets import load_dataset
from transformers import AutoTokenizer

from conf import config
def process():
    #读取数据/删除多余列/划分数据集
    dataset=load_dataset(path='json',data_files=str(config.DATA_DIR/'ner'/'raw'/'data.json'))['train']
    dataset_removed=dataset.remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])
    dataset_split=dataset_removed.train_test_split(test_size=0.2)
    dataset_split['val'],dataset_split['test']=dataset_split['test'].train_test_split(test_size=0.5).values()

    #创建Tokenizer
    tokenizer=AutoTokenizer.from_pretrained(config.MODEL_NAME)

    #label映射关系
    id2label = ['B', 'I', 'O']
    label2id = {label: id for id,label in enumerate(id2label)}

    #对数据集分词并填充

    #map函数
    def map_fun(example):
        tokens = list(example['text'])
        tokens=[text.replace(' ','') for text in tokens]
        labels = [label2id['O']]*len(tokens)
        #对text按字分词并处理
        a = tokenizer(tokens, is_split_into_words=True,truncation=True)
        entities = example.get('label',[])
        if entities:
            for entity in entities:
                start=entity['start']
                end=entity['end']
                labels[start:end]=[label2id['B']]+[label2id['I']]*(end-start-1)
                a['labels']= [-100]+labels+[-100]
        else:
            a['labels']=[-100]+[2]*(len(tokens))+[-100]
        return a

    dataset_dict=dataset_split.map(map_fun,batched=False,remove_columns=['text','label'],)
    dataset_dict.save_to_disk(config.DATA_DIR/'ner'/'processed')
if __name__ == '__main__':
    process()