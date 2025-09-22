# build_prerequisites.py
# 构建课程前置关系：A:B 表示 B 是 A 的前置课程
# 改进：强制模型只输出“是”或“否”，避免文本解析

import json
import pymysql
from dashscope import Generation
import dashscope
from tqdm import tqdm
import time
import os

# 🔧 配置
dashscope.api_key = 'sk-0aa74c2785824d4cbe3c509ff607d383'  # ⚠️ 替换为你的 API Key
MODEL_NAME = 'qwen-max'
KP_FILE = 'kp_for_course.txt'
OUTPUT_FILE = 'pre_points.txt'

# 🛠️ 数据库配置（请替换为你的实际信息）
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '17751817',
    'database': 'edu_ai',
    'charset': 'utf8mb4'
}

DEBUG = True  # 开启调试日志
_call_counter = 0

def load_course_subjects_from_db():
    """从 MySQL course_info 表加载 course_id → subject_id 映射"""
    print("📊 正在从 MySQL 加载 course_info 表...")
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, subject_id FROM course_info"
            cursor.execute(sql)
            results = cursor.fetchall()
            # 返回 dict: course_id -> subject_id
            return {str(row[0]).strip(): str(row[1]).strip() for row in results}
    except Exception as e:
        print(f"❌ 数据库查询失败: {e}")
        return {}
    finally:
        connection.close()


def load_kp_data(file_path):
    """加载知识点数据"""
    print("📂 正在加载 kp_for_course.txt...")
    data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    course_id = str(item['id']).strip()
                    sub_points = item.get('sub_points', [])
                    if isinstance(sub_points, str):
                        sub_points = [sub_points]
                    elif not isinstance(sub_points, list):
                        sub_points = []
                    data[course_id] = {
                        'main_point': item['main_point'],
                        'sub_points': sub_points
                    }
                except Exception as e:
                    print(f"❌ 第 {line_num} 行解析失败: {line[:50]}... -> {e}")
        print(f"✅ 成功加载 {len(data)} 门课程的知识点")
        return data
    except FileNotFoundError:
        print(f"❌ 文件未找到: {file_path}")
        exit(1)


def is_prequisite(pre_data, tgt_data):
    """
    判断 pre_data 是否是 tgt_data 的前置课程
    即：是否建议先学 A（pre），再学 B（tgt）？
    模型必须只输出“是”或“否”
    """
    global _call_counter
    _call_counter += 1

    pre_main = pre_data['main_point']
    pre_subs = ', '.join(pre_data['sub_points']) if pre_data['sub_points'] else '无'
    tgt_main = tgt_data['main_point']
    tgt_subs = ', '.join(tgt_data['sub_points']) if tgt_data['sub_points'] else '无'

    prompt = f"""
    【任务】请严格判断：课程A是否是学习课程B的**必要前置知识**。

    即：如果不掌握课程A的内容，是否会导致无法理解或掌握课程B的核心内容？

    【课程A：候选前置】
    主题: {pre_main}
    内容: {pre_subs}

    【课程B：目标课程】
    主题: {tgt_main}
    内容: {tgt_subs}

    【判断标准】
    - 如果课程A是课程B的**基础性、必要性知识**（例如：不懂A就看不懂B），则输出：“是”
    - 如果课程A只是“相关工具”、“可选技能”、“后期拓展”，或“虽有用但非必需”，则输出：“否”
    - 特别注意：构建工具（如Maven）、版本控制（如Git）、操作系统（如Linux）通常不是编程语言基础的前置，除非课程B明确涉及底层系统编程

    【输出要求】
    - 仅输出一个字：“是” 或 “否”
    - 禁止解释、禁止额外文字
    - 输出必须是单个汉字

    【回答】
    """

    if DEBUG and _call_counter <= 5:
        print(f"\n" + "=" * 60)
        print(f"📌 判断 #{_call_counter}")
        print(f"🔍 前置课程: {pre_main}")
        print(f"🎯 目标课程: {tgt_main}")
        print(f"💡 模型输入已生成（长度: {len(prompt)} 字符）")

    for _ in range(2):  # 最多重试 2 次
        try:
            response = Generation.call(
                model=MODEL_NAME,
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.2
            )
            if response.status_code == 200:
                raw_output = response.output['text'].strip()
                if DEBUG and _call_counter <= 5:
                    print(f"🧠 模型输出: '{raw_output}'")

                # 🔥 严格匹配：必须是“是”或“否”
                if raw_output == '是':
                    return True
                elif raw_output == '否':
                    return False
                else:
                    if DEBUG:
                        print(f"⚠️  格式错误（期望“是”或“否”）: '{raw_output}'")
                    continue  # 重试
            else:
                print(f"❌ API 错误: {response.status_code} {response.message}")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 调用异常: {e}")
            time.sleep(2)

    # 重试失败，返回 False
    return False


# ========================
# 主流程
# ========================
if __name__ == "__main__":
    print(f"📁 当前工作目录: {os.getcwd()}")

    # 1. 加载数据库映射
    subject_map = load_course_subjects_from_db()
    if not subject_map:
        print("❌ 未从数据库加载到任何数据，退出")
        exit(1)

    # 2. 加载知识点
    kp_data = load_kp_data(KP_FILE)
    if not kp_data:
        print("❌ 未加载到任何知识点数据，退出")
        exit(1)

    # 3. 过滤有效课程（同时存在于数据库和知识点文件）
    valid_courses = {cid: subj for cid, subj in subject_map.items() if cid in kp_data}
    print(f"✅ 共 {len(valid_courses)} 门有效课程")

    # 4. 按 subject_id 分组
    subject_groups = {}
    for cid, subj in valid_courses.items():
        subject_groups.setdefault(subj, []).append(cid)

    print(f"✅ 分为 {len(subject_groups)} 个学科组")

    # 5. 清空输出文件
    try:
        open(OUTPUT_FILE, 'w').close()
        print(f"🗑️  已清空 {OUTPUT_FILE}")
    except Exception as e:
        print(f"❌ 无法清空文件: {e}")
        exit(1)

    # 6. 遍历每个学科组
    total_relations = 0
    for subject_id, course_ids in subject_groups.items():
        n = len(course_ids)
        if n < 2:
            print(f"🟡 学科 {subject_id} 只有 {n} 门课，跳过")
            continue

        print(f"🔍 处理学科 {subject_id}（{n} 门课程）...")
        total_pairs = n * (n - 1)
        pbar = tqdm(total=total_pairs, desc=f"学科 {subject_id}", unit="pair")

        # 以追加模式打开文件，实时写入
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f_out:
            for target_id in course_ids:
                for pre_id in course_ids:
                    if target_id == pre_id:
                        pbar.update(1)
                        continue

                    # 判断：pre_id 是否是 target_id 的前置？
                    if is_prequisite(kp_data[pre_id], kp_data[target_id]):
                        line = f"{target_id}:{pre_id}\n"
                        f_out.write(line)
                        f_out.flush()  # 强制写入磁盘
                        total_relations += 1
                        if DEBUG:
                            print(f"✅ 发现前置关系: {target_id}:{pre_id}")
                    else:
                        if DEBUG:
                            print(f"❌ 无前置关系: {target_id} ←× {pre_id}")
                    pbar.update(1)
            f_out.flush()

        pbar.close()
        print(f"✅ 学科 {subject_id} 处理完成")

    print(f"\n🎉 全部完成！共发现 {total_relations} 条前置关系")
    print(f"📄 格式说明: A:B 表示 B 是 A 的前置课程")
    print(f"💾 结果已实时写入: {os.path.abspath(OUTPUT_FILE)}")