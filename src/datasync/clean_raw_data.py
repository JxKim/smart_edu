

import json
from configuration import config


def clean_data():
    # 1. 读取原始标注数据
    with open(str(config.DATA_DIR / 'ner' / 'raw' / 'edu_data.json'), 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    raw_count = len(raw_data)
    print(f"开始数据清洗，原始数据总量：{raw_count} 条")

    # 2. 初始化用于清洗的变量
    cleaned_data = []  # 存储最终清洗后的数据
    seen_texts = set()  # 记录已出现过的text，用于去重
    invalid_label_count = 0  # 统计「无label/无效label」的数据量
    duplicate_count = 0  # 统计「重复text」的数据量

    # 3. 逐行处理数据
    for item in raw_data:
        # 3.1 过滤「无label字段」的数据
        if 'label' not in item or not item['label']:  # 兼容label为None/空列表的情况
            invalid_label_count += 1
            continue

        # 3.2 过滤「包含无效label」的数据（无效：label的text是空字符串/单字符）
        has_invalid = False
        for label in item['label']:
            # 检查label的text是否为有效字符串
            label_text = label.get('text', '').strip()  # 去除首尾空格（避免“  a  ”这类误判）
            if len(label_text) <= 1:  # 空字符串或单字符均视为无效
                has_invalid = True
                break
        if has_invalid:
            invalid_label_count += 1
            continue

        # 3.3 过滤「重复text」的数据
        current_text = item.get('text', '').strip()  # 建议strip：避免“text带首尾空格”导致的假重复
        if current_text in seen_texts:
            duplicate_count += 1
            continue
        # 首次出现的text：加入去重集合，并保留数据
        seen_texts.add(current_text)
        cleaned_data.append(item)

    # 4. 保存清洗后的数据
    with open(str(config.DATA_DIR / 'ner' / 'raw' / 'cleaned_edu_data.json'), 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)  # indent=2：格式化输出，方便查看

    # 5. 打印清洗结果日志
    cleaned_count = len(cleaned_data)
    print("=" * 50)
    print("数据清洗结果汇总：")
    print(f"原始数据：{raw_count} 条")
    print(f"过滤无label/无效label：{invalid_label_count} 条")
    print(f"过滤重复text：{duplicate_count} 条")
    print(f"最终清洗后数据：{cleaned_count} 条") #❗5079
    print(f"清洗后数据已保存至：{config.DATA_DIR / 'ner' / 'raw' / 'cleaned_edu_data.json'}")


'''
开始数据清洗，原始数据总量：5762 条
==================================================
数据清洗结果汇总：
原始数据：5762 条
过滤无label/无效label：159 条
过滤重复text：524 条
最终清洗后数据：5079 条
'''

if __name__ == '__main__':
    clean_data()