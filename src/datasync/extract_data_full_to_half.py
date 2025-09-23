#❗写入txt时需要转换为全角→半角 否则出现单个字母是一个实体的情况 然后用label-studio进行标注 最终得到data/raw路径下的json文件
#❗还要去掉空格 否则tokenizer标签对齐很难处理
import pymysql
import unicodedata  # 引入处理Unicode的模块

from configuration import config


# 全角转半角函数
def full_to_half(s):
    """将字符串中的全角字符转换为半角"""
    result = []
    for char in s:
        # 标准化字符（处理一些特殊全角字符）
        normalized_char = unicodedata.normalize('NFKC', char)
        # 全角字符的Unicode范围（除空格外）
        if '\uFF01' <= normalized_char <= '\uFF5E':
            # 全角转半角：全角字符与半角字符的Unicode值相差0xfee0
            result.append(chr(ord(normalized_char) - 0xfee0))
        elif normalized_char == '\u3000':  # 全角空格
            result.append(' ')  # 转换为半角空格
        else:
            result.append(normalized_char)
    return ''.join(result)


def process_and_write():
    conn = pymysql.connect(**config.SQL_CONFIG)
    cursor = conn.cursor()

    # 从chapter_info抽取chapter_name
    sql_table1 = '''select chapter_name from chapter_info'''
    cursor.execute(sql_table1)
    results1 = cursor.fetchall()

    # 从course_info抽取course_introduce
    sql_table2 = '''select course_introduce from course_info'''
    cursor.execute(sql_table2)
    results2 = cursor.fetchall()

    cursor.close()
    conn.close()

    with open(str(config.DATA_DIR / 'desc_full_to_half.txt'), 'w', encoding='utf-8') as f:
        # 处理并写入chapter_name数据
        for row in results1:
            # 步骤1：全角转半角 → 步骤2：去除所有空格 → 步骤3：写入文件
            processed = full_to_half(row[0]).replace(' ', '')
            if processed:  # 跳过空行（可选）
                f.write(processed + '\n')

        # 处理并写入course_introduce数据
        for row in results2:
            processed = full_to_half(row[0]).replace(' ', '')
            if processed:  # 跳过空行（可选）
                f.write(processed + '\n')

    print('sql数据转半角并去除空格成功~')


if __name__ == '__main__':
    process_and_write()
