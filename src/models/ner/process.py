from dotenv import load_dotenv
load_dotenv()
from datasets import load_dataset
from transformers import AutoTokenizer

from configuration import config

def process():
    # 读取数据
    dataset = load_dataset('json',data_files=str(config.DATA_DIR/'ner'/'raw'/'knowledge.json'))['train']
    dataset = dataset.remove_columns(['id' ,'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])

    # 划分数据集
    dataset_dict = dataset.train_test_split(test_size=0.2)
    dataset_dict['test'],dataset_dict['valid'] = dataset_dict['test'].train_test_split(test_size=0.5).values()
    print(dataset_dict)

    # 数据编码
    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)
    def map_func(example):
        tokens = list(example['text'])
        entities = example['label']

        # 创建字符级别的标签
        char_labels = [config.LABELS.index('O')] * len(tokens)
        for entity in entities:
            start = entity['start']
            end = entity['end']
            # 确保实体在范围内
            if start < len(char_labels) and end <= len(char_labels):
                char_labels[start] = config.LABELS.index('B')
                for i in range(start + 1, end):
                    if i < len(char_labels):
                        char_labels[i] = config.LABELS.index('I')

        # Tokenize（字符级分词）
        inputs = tokenizer(
            tokens,
            truncation=True,
            is_split_into_words=True,
            return_offsets_mapping=True  # 关键：获取偏移量映射
        )

        # 获取word_ids用于对齐
        word_ids = inputs.word_ids()

        # 对齐标签
        aligned_labels = []
        for i, word_id in enumerate(word_ids):
            if word_id is None:
                # 特殊token ([CLS], [SEP]等)
                aligned_labels.append(-100)
            else:
                # 对应原始字符的标签
                aligned_labels.append(char_labels[word_id])

        inputs['labels'] = aligned_labels

        # 移除不需要的offset_mapping
        inputs.pop('offset_mapping')

        return inputs
    dataset_dict = dataset_dict.map(map_func,remove_columns=['text','label'])
    dataset_dict.save_to_disk(str(config.DATA_DIR/'ner'/'processed'))
if __name__ == '__main__':
    process()