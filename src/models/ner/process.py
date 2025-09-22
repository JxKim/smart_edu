import dotenv
from datasets import load_dataset
from transformers import AutoTokenizer

from configuration import config

dotenv.load_dotenv()


def process():
    # 加载数据集
    dataset = load_dataset("json", data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'data.json'))['train']
    dataset = dataset.remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])
    print(dataset)

    # 划分数据集
    dataset_dict = dataset.train_test_split(test_size=0.2)
    dataset_dict['test'], dataset_dict['valid'] = dataset_dict['test'].train_test_split(test_size=0.5).values()
    print(dataset_dict)

    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)
    id2label = ['B', 'I', 'O']
    label2id = {label: i for i, label in enumerate(id2label)}

    def tokenize(example):
        tokens = list(example['text'])
        # 使用return_offsets_mapping来获取token到原始文本的映射
        inputs = tokenizer(
            tokens,
            truncation=True,
            is_split_into_words=True,
            return_offsets_mapping=True
        )
        
        # 初始化标签，全部设为'O'
        labels = [label2id['O']] * len(inputs['input_ids'])
        
        # 将特殊token位置的标签设为-100
        if 'special_tokens_mask' in inputs:
            for i, special_token_mask in enumerate(inputs['special_tokens_mask']):
                if special_token_mask:
                    labels[i] = -100
        else:
            # 手动设置特殊token位置([CLS]和[SEP])
            labels[0] = -100
            if len(labels) > 1:
                labels[-1] = -100
        
        # 处理实体标签
        entities = example.get('label', [])
        if entities:
            word_ids = inputs.word_ids()
            for entity in entities:
                start_char = entity['start']
                end_char = entity['end']
                
                # 找到对应token的位置
                token_start, token_end = None, None
                for i, word_id in enumerate(word_ids):
                    if word_id is not None:
                        word_start, word_end = inputs['offset_mapping'][i]
                        # 检查token是否与实体边界对齐
                        if word_start == start_char:
                            token_start = i
                        if word_end == end_char:
                            token_end = i
                
                # 如果找到了开始和结束位置，则标记实体
                if token_start is not None and token_end is not None:
                    labels[token_start] = label2id['B']
                    for i in range(token_start + 1, token_end + 1):
                        labels[i] = label2id['I']
        
        inputs['labels'] = labels
        # 删除offset_mapping，因为它在训练中不需要
        del inputs['offset_mapping']
        return inputs

    dataset_dict = dataset_dict.map(tokenize, batched=False, remove_columns=['text', 'label'])
    print(dataset_dict)
    dataset_dict.save_to_disk(str(config.DATA_DIR / 'ner' / 'processed'))


if __name__ == '__main__':
    process()
