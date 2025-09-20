from dotenv import load_dotenv

load_dotenv()
from datasets import load_dataset

from transformers import AutoTokenizer

from configuration import config


def process():
    # 读取数据
    dataset = load_dataset('json', data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'data.json'))['train']
    dataset = dataset.remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])

    # 划分数据集
    dataset_dict = dataset.train_test_split(test_size=0.2)
    dataset_dict['test'], dataset_dict['valid'] = dataset_dict['test'].train_test_split(test_size=0.5).values()

    # 数据编码
    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)

    def map_func(example):
        tokens = list(example['text'])
        inputs = tokenizer(tokens, is_split_into_words=True)

        # 获取实体标签，确保它是可迭代的（默认为空列表）
        entities = example.get('label', [])
        if entities is None:
            entities = []

        labels = [config.LABELS.index('O')] * len(tokens)
        for entity in entities:  # 现在这里不会再出错了
            start = entity['start']
            end = entity['end']
            labels[start:end] = [config.LABELS.index('B')] + [config.LABELS.index('I')] * (end - start - 1)

        labels = [-100] + labels + [-100]
        inputs['labels'] = labels
        return inputs

    dataset_dict = dataset_dict.map(map_func, remove_columns=['text', 'label'])

    dataset_dict.save_to_disk(str(config.DATA_DIR / 'ner' / 'processed'))

if __name__ == '__main__':
    process()
