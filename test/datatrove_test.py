#!/usr/bin/env python3
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.writers import JsonlWriter
from datatrove.executor import LocalPipelineExecutor
import re


# 1. 定义一个简单的清洗函数（例如，去除多余空格和特殊字符）
def clean_text(text):
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)  # 移除控制字符
    text = re.sub(r'\s+', ' ', text).strip()  # 将多个空白符替换为一个空格
    return text


# 2. 创建处理管道
pipeline = [
    # 读取器：从指定目录读取jsonl文件
    JsonlReader(
        data_folder="./question_txt.txt",  # 替换为你的输入数据目录
        text_key="text",  # 指定文本字段的键名
        # id_key="id",               # 指定ID字段的键名（若存在）
        recursive=False,  # 是否递归读取子目录
        glob_pattern="*.jsonl"  # 文件匹配模式
    ),

    # 过滤器：使用Lambda函数进行自定义清洗或过滤
    LambdaFilter(
        lambda doc: clean_text(doc.text)  # 应用清洗函数
    ),

    # 你可以在这里添加更多过滤器，例如：
    # - 语言识别过滤 (LanguageFilter)
    # - 质量分数过滤 (QualityFilter)
    # - 敏感信息过滤 (LambdaFilter 配合正则)
    # - 去重 (MinHashDeduplication)

    # 写入器：将结果写入指定目录
    JsonlWriter(
        output_folder="./cleaned_output",  # 替换为你的输出目录
        output_filename="${rank}.jsonl.gz",  # 输出文件名模式
        compression="gzip"  # 压缩格式
    )
]

# 3. 创建并运行本地执行器
executor = LocalPipelineExecutor(
    pipeline=pipeline,
    tasks=4,  # 任务数量（通常可设置为CPU核心数）:cite[1]
    workers=1,  # 每个任务的工作线程数（通常CPU密集型任务设为1以避免GIL问题）:cite[1]
    logging_dir="./logs",  # 日志目录
    start_method="spawn"  # Windows系统下需设置为"spawn":cite[4]
)

if __name__ == "__main__":
    print("开始数据处理...")
    executor.run()
    print("数据处理完成！")