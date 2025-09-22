import dotenv

dotenv.load_dotenv()
from datasets import load_dataset
from transformers import AutoTokenizer

from configuration import config


def process():
    # 1. 读取数据
    dataset = load_dataset('json', data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'cleaned_edu_data.json'))[
        'train']  # ❗5401条数据
    # print(dataset) # features: ['text', 'id', 'label', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'],
    dataset = dataset.remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])
    # print(dataset) # features: ['text', 'label']

    # 2.划分数据集
    dataset = dataset.train_test_split(test_size=0.2, shuffle=True, seed=42)
    dataset['val'], dataset['test'] = dataset['test'].train_test_split(test_size=0.5, shuffle=True, seed=42).values()
    # import pdb;pdb.set_trace()
    # 3. 数据编码 处理分词和标签对齐
    tokenizer = AutoTokenizer.from_pretrained(config.PRE_MODEL_NAME)

    def map_func(example):
        lower_text = example['text'].lower().replace(' ', '')  # ❗转为小写 bert-base-chinese词表无大写英文字母 + 去掉空格
        tokens = list(lower_text)
        inputs = tokenizer(tokens, truncation=True, is_split_into_words=True)
        entities = example['label']
        labels = [config.LABELS.index('O')] * len(tokens)
        for entity in entities:
            start = entity['start']
            end = entity['end']
            labels[start:end] = [config.LABELS.index('B')] + [config.LABELS.index('I')] * (end - start - 1)
        labels = [-100] + labels + [-100]
        inputs['labels'] = labels
        # 检查长度是否匹配
        # 检查长度是否匹配，同时打印原始文本
        if len(inputs['input_ids']) != len(inputs['labels']):
            print(f"❌ 长度不匹配:")
            print(f"  原始文本: {example['text']}")  # 输出原始句子，方便定位
            print(f"  小写后文本: {lower_text}")  # 输出小写后文本，辅助分析
            print(f"  input_ids长度: {len(inputs['input_ids'])}, labels长度: {len(inputs['labels'])}")
            print(f"  分词后token: {tokenizer.convert_ids_to_tokens(inputs['input_ids'])}")  # 输出分词结果，看是否有子词拆分
            print("-" * 50)  # 分隔线，方便阅读日志
        return inputs

    dataset = dataset.map(map_func, remove_columns=['text', 'label'])
    dataset.save_to_disk(str(config.DATA_DIR / 'ner' / 'processed'))


if __name__ == '__main__':
    process()
