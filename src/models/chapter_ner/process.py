from dotenv import load_dotenv
load_dotenv()
from datasets import load_dataset
from transformers import AutoTokenizer

from configuration import config


def process():
    # 读取数据
    dataset = load_dataset('json', data_files=str(config.DATA_DIR_RAW / 'chapter.json'))['train']
    dataset = dataset.remove_columns([ 'id',  'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])
    # print( dataset)
    # 划分数据集
    dataset_dict = dataset.train_test_split(test_size=0.2)
    dataset_dict['test'], dataset_dict['valid'] = dataset_dict['test'].train_test_split(test_size=0.5).values()
    # 数据处理
    tokenizer = AutoTokenizer.from_pretrained('google-bert/bert-base-chinese')

    # def map_func(example):
    #     tokens = list(example['text'])
    #     inputs = tokenizer(tokens, truncation=True, is_split_into_words=True)
    #     entities = example['label']
    #     # labels = [config.LABELS.index('O')] * len(inputs['input_ids'])
    #     labels = [config.LABELS.index('O')] * len(tokens)
    #     for entity in entities:
    #         start = entity['start']
    #         end = entity['end']
    #         labels[start:end] = [config.LABELS.index('B')] + [config.LABELS.index('I')] * (end - start - 1)
    #     labels = [-100] + labels + [-100]
    #     # labels[0] = -100
    #     # labels[-1] = -100
    #     inputs['labels'] = labels
    #     return inputs

    def map_func(example):
        tokens = list(example['text'])
        entities = example['label']

        # 创建字符级标签
        char_labels = [config.LABELS.index('O')] * len(tokens)
        for entity in entities:
            start = entity['start']
            end = entity['end']
            char_labels[start:end] = [config.LABELS.index('B')] + [config.LABELS.index('I')] * (end - start - 1)

        # 使用分词器并获取单词到字符的映射
        inputs = tokenizer(
            tokens,
            truncation=True,
            is_split_into_words=True,
            return_offsets_mapping=True  # 获取字符到标记的映射
        )

        # 创建与input_ids长度相同的标签列表，初始化为-100
        labels = [-100] * len(inputs["input_ids"])

        # 获取偏移映射
        offset_mapping = inputs.pop("offset_mapping")

        # 将字符级标签映射到标记级标签
        for i, (start, end) in enumerate(offset_mapping):
            # 跳过特殊标记([CLS], [SEP], etc.)
            if start == end == 0:
                continue

            # 如果当前标记对应单个字符，直接使用该字符的标签
            if end - start == 1:
                labels[i] = char_labels[start]
            # 如果当前标记对应多个字符，使用第一个字符的标签
            # (或者可以根据需要调整策略)
            else:
                labels[i] = char_labels[start]

        inputs['labels'] = labels
        return inputs

    dataset_dict = dataset_dict.map(map_func,remove_columns=['text','label'])

    for item in dataset_dict['train']:
        labels = len(item['labels'])
        inputs_idss = len(item['input_ids'])
        # print(f"labels:{labels} inputs_ids:{inputs_idss}")
        # print("=========="*5)
        if labels != inputs_idss:
            print('=========='*10,'发现长度不一致')

    # print(dataset_dict)
    print(dataset_dict['train'][0])

    dataset_dict.save_to_disk(str(config.DATA_DIR_PROCESSED / 'chapter'))

if __name__ == '__main__':
    process()