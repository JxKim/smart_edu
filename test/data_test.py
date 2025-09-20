import warnings

warnings.filterwarnings("ignore")

import pymysql
from pymysql.cursors import DictCursor

from configuration import config

import re
import zhconv
from takin import delete_escape_character, delete_extra_whitespace, delete_digit


def takin_clean(dirty_text):
    cleaned_text = delete_escape_character(dirty_text)  # 删除转义字符
    cleaned_text = delete_extra_whitespace(cleaned_text)  # 删除多余空白
    cleaned_text = delete_digit(cleaned_text)  # 删除数字
    return cleaned_text


def clean_text(text, to_traditional=False):
    """
    综合文本清洗函数
    :param text: 原始文本
    :param to_traditional: 是否转换为繁体，默认False即转简体
    :return: 清洗后的文本
    """
    # 1. 移除控制字符、BOM
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)
    text = re.sub(r'^\uFEFF', '', text)

    # 2. 移除HTML标签、URL、Email等
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'http\S+|www\.\S+', '', text)
    text = re.sub(r'\S+@\S+', '', text)
    text = re.sub(r'[#@]\S+', '', text)

    # 3. 繁体转简体 (默认)
    if not to_traditional:
        text = zhconv.convert(text, 'zh-cn')
    # else:
    #     text = zhconv.convert(text, 'zh-tw')  # 如需转繁体

    # 4. 处理重复标点 (例如：将多个！替换成一个！)
    text = re.sub(r'([!！?？。\.])\1+', r'\1', text)
    # 处理省略号（将多个连续的。或..等替换成标准…）
    text = re.sub(r'[\.。]{2,}', '…', text)

    # 5. (可选) 移除所有非必要字符，只保留中文、英文、数字、常用标点
    # 这是一个比较严格的正则，请根据你的数据谨慎修改和使用
    # kept_chars = r'\u4e00-\u9fa5\w\s\!！\?？\.。，,；;：:"“”''‘’《》〈〉()（）【】\[\]…\-'
    # text = re.sub(f'[^{kept_chars}]', '', text)

    # 6. 标准化空格：将任何空白字符（包括全角空格）转换为一个半角空格，然后去除首尾空格
    text = re.sub(r'\s+', '', text)
    text = text.strip()

    return text


def extract_data():
    connection = pymysql.connect(**config.MYSQL_CONFIG)
    cursor = connection.cursor(DictCursor)
    sql = """
        select chapter_name
        from chapter_info limit 1000
        """
    cursor.execute(sql)
    chapter_name = cursor.fetchall()
    with open('chapter_name.txt', 'a', encoding='utf-8') as f:
        for i in chapter_name:
            print(i['chapter_name'])
            text = i['chapter_name'].replace(' ', '')
            f.write(text)
            f.write('\n')


def extract_question():
    connection = pymysql.connect(**config.MYSQL_CONFIG)
    cursor = connection.cursor(DictCursor)
    sql = """
            select question_txt
            from test_question_info
            """
    cursor.execute(sql)
    chapter_name = cursor.fetchall()
    with open('question_txt.txt', 'a', encoding='utf-8') as f:
        for i in chapter_name:
            # print(i['chapter_name'])
            # text = i['chapter_name'].replace(' ', '')
            # f.write(text)
            f.write(i['question_txt'])
            f.write('\n')


if __name__ == '__main__':
    # dirty_text = "今 天 天 气真好！！！！！<br>欢迎访问http://example.com :) Ｈｅｌｌｏ　Ｗｏｒｌｄ"
    # dirty_text = "<p>&lt;p&gt;&lt;strong&gt;下面哪些函数是public void aMethod(){...}的重载函数?&lt;/strong&gt;&lt;/p&gt;</p>"
    # clean_text = takin_clean(dirty_text)
    # print(clean_text)  # 输出：今天天气真好！欢迎访问 :) Hello World

    # 使用示例
    # dirty_text = "今 天 天 气真好！！！！！<br>欢迎访问http://example.com :) Ｈｅｌｌｏ　Ｗｏｒｌｄ"
    # dirty_text = "<p>&lt;p&gt;&lt;strong&gt;下面哪些函数是public void aMethod(){...}的重载函数?&lt;/strong&gt;&lt;/p&gt;</p>"
    # clean_text = clean_text(dirty_text)
    # print(clean_text)  # 输出：今天天气真好！欢迎访问 :) Hello World

    # extract_data()
    extract_question()
