from dotenv import load_dotenv
import logging

load_dotenv()
from datasets import load_dataset

from transformers import AutoTokenizer

from configuration import config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process():
    # 读取数据
    dataset = load_dataset('json', data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'data.json'))['train']
    logger.info(f"原始数据集: {dataset}")

    # 移除不需要的列
    columns_to_remove = ['id', 'file_upload', 'drafts', 'predictions', 'meta', 'created_at',
                         'updated_at', 'inner_id', 'total_annotations', 'cancelled_annotations',
                         'total_predictions', 'comment_count', 'unresolved_comment_count',
                         'last_comment_updated_at', 'project', 'updated_by', 'comment_authors']
    dataset = dataset.remove_columns(columns_to_remove)

    # 划分数据集
    dataset_dict = dataset.train_test_split(test_size=0.2, seed=42)
    dataset_dict['test'], dataset_dict['valid'] = dataset_dict['test'].train_test_split(
        test_size=0.5, seed=42
    ).values()
    logger.info(f"数据集划分完成: {dataset_dict}")

    # 加载分词器
    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)
    logger.info(f"使用分词器: {config.MODEL_NAME}")

    def map_func(example):
        # 获取文本
        text = example['data']['text']
        original_length = len(text)

        # 处理实体标注
        labels = [config.LABELS.index('O')] * original_length

        if example['annotations'] and len(example['annotations']) > 0:
            annotations = example['annotations'][0]
            if 'result' in annotations and len(annotations['result']) > 0:
                for entity in annotations['result']:
                    entity_value = entity['value']
                    start = entity_value['start']
                    end = entity_value['end']

                    # 边界检查
                    if start < 0 or end > original_length:
                        logger.warning(f"实体位置超出文本范围: start={start}, end={end}, 文本长度={original_length}")
                        continue

                    # 设置标签
                    if start < len(labels):
                        labels[start] = config.LABELS.index('B')
                        for i in range(start + 1, end):
                            if i < len(labels):
                                labels[i] = config.LABELS.index('I')

        # 分词处理 - 关键修正：不预先拆分字符，让分词器处理
        inputs = tokenizer(
            text,
            truncation=True,
            max_length=config.MAX_SEQ_LENGTH if hasattr(config, 'MAX_SEQ_LENGTH') else 512,
            return_offsets_mapping=True  # 获取偏移量映射，用于对齐标签
        )

        # 根据分词结果重新生成标签 - 关键修正
        offset_mapping = inputs.pop('offset_mapping')  # 移除偏移量映射，不需要保存
        new_labels = []
        previous_offset = -1

        for offset in offset_mapping:
            # 特殊符号位置（CLS, SEP, PAD）
            if offset[0] == 0 and offset[1] == 0:
                new_labels.append(-100)
            else:
                # 取第一个字符的标签
                if offset[0] < original_length and offset[0] != previous_offset:
                    new_labels.append(labels[offset[0]])
                    previous_offset = offset[0]
                else:
                    # 子词重复使用主词标签
                    new_labels.append(new_labels[-1] if new_labels else -100)

        inputs['labels'] = new_labels

        # 长度校验 - 关键新增
        if len(inputs['input_ids']) != len(inputs['labels']):
            logger.error(
                f"长度不匹配: input_ids长度={len(inputs['input_ids'])}, "
                f"labels长度={len(inputs['labels'])}, 文本: {text[:50]}..."
            )
            # 尝试修正（取较短的长度）
            min_len = min(len(inputs['input_ids']), len(inputs['labels']))
            inputs['input_ids'] = inputs['input_ids'][:min_len]
            inputs['attention_mask'] = inputs['attention_mask'][:min_len]
            inputs['labels'] = inputs['labels'][:min_len]

        return inputs

    # 应用映射函数
    dataset_dict = dataset_dict.map(
        map_func,
        remove_columns=['data', 'annotations'],
        num_proc=4  # 多进程处理
    )

    # 保存前进行最终校验
    def validate_dataset(dataset, dataset_name):
        invalid_count = 0
        for i, item in enumerate(dataset):
            if len(item['input_ids']) != len(item['labels']):
                logger.warning(
                    f"{dataset_name}集样本 {i} 长度不匹配: "
                    f"input_ids={len(item['input_ids'])}, labels={len(item['labels'])}"
                )
                invalid_count += 1
        if invalid_count == 0:
            logger.info(f"{dataset_name}集所有样本长度校验通过")
        else:
            logger.warning(f"{dataset_name}集共发现 {invalid_count} 个无效样本")
        return invalid_count == 0

    # 校验各数据集
    validate_dataset(dataset_dict['train'], '训练')
    validate_dataset(dataset_dict['valid'], '验证')
    validate_dataset(dataset_dict['test'], '测试')

    # 保存处理后的数据集
    save_path = str(config.DATA_DIR / 'ner' / 'processed')
    dataset_dict.save_to_disk(save_path)
    logger.info(f"处理后的数据集已保存至: {save_path}")


if __name__ == '__main__':
    process()
