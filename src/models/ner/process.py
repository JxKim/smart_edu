import pymysql
from datasets import load_dataset,Features,Value,Sequence
import transformers
from datasets import load_from_disk

from src.configuration import config


def export_txt():
    conn=pymysql.connect(**config.MYSQL_CONFIG)
    cursor=conn.cursor()
    sql='''
    select chapter_name
    from chapter_info
    '''
    cursor.execute(sql)
    texts=cursor.fetchall()
    texts=[text[0] for text in texts]
    with open(config.DATA_DIR/'ner'/'raw'/'chapter.txt','w',encoding='utf-8') as f:
        f.write('\n'.join(texts))

def process():
    dataset = (load_dataset(path='json', data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'data.json'))
               ['train'].select(list(range(1000))).remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time']))

    # print(dataset)

    dataset_dict = dataset.train_test_split(test_size=0.2)
    dataset_dict['test'], dataset_dict['valid'] = dataset_dict['test'].train_test_split(test_size=0.5).values()
    tokenzier = transformers.AutoTokenizer.from_pretrained('google-bert/bert-base-chinese')


    def tokenize(example):
        text_list = list(example['text'])
        inputs = tokenzier(text_list, truncation=True, is_split_into_words=True)
        labels = [config.LABELS.index('O')] * len(text_list)
        if example['label'] and example['label'][0]['text']:
            for label in example['label']:
                start = label['start']
                end = label['end']
                labels[start] = config.LABELS.index('B')
                labels[start + 1:end] = [config.LABELS.index('I')] * (end - start - 1)
        word_idx=inputs.word_ids()
        labels=[labels[idx] for idx in word_idx if idx]
        inputs['labels'] = [-100] + labels + [-100]
        return inputs

    dataset_dict = dataset_dict.map(tokenize,batched=False,remove_columns=['text', 'label'])
    # print(dataset_dict['train'][0:2])
    dataset_dict.save_to_disk(config.DATA_DIR / 'ner' / 'processed')

if __name__ == '__main__':
    # export_txt()
    process()
    # train_dataset = load_from_disk(config.DATA_DIR / 'ner' / 'processed' / 'train')
    # print(len(train_dataset[0]['input_ids']),len(train_dataset[0]['attention_mask']),
    #       len(train_dataset[0]['token_type_ids']),len(train_dataset[0]['labels']))



