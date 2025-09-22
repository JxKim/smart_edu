import time
import json
import re
from dashscope import Generation
import dashscope
from tqdm import tqdm

dashscope.api_key = 'sk-0aa74c2785824d4cbe3c509ff607d383'  # 替换为你的 DashScope API Key
MODEL_NAME = 'qwen-max'  # 推荐使用 qwen-max，推理更准确
INPUT_FILE = 'course_introduce.txt'
OUTPUT_FILE = 'kp_for_course.txt'

DEBUG = False

def robust_parse_json(text):
    try:
        # 尝试直接解析
        return json.loads(text)
    except:
        pass

    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        return None

    json_str = match.group()

    # 修复常见问题
    json_str = re.sub(r',\s*}', '}', json_str)  # 去除末尾逗号
    json_str = re.sub(r',\s*\]', ']', json_str)
    json_str = json_str.replace('“', '"').replace('”', '"')  # 中文引号
    json_str = json_str.replace('：', ':')  # 中文冒号

    try:
        return json.loads(json_str)
    except Exception as e:
        print(f"JSON 解析失败: {e}")
        return None


def extract_knowledge_from_introduce(course_id, introduce_text):
    """
    调用大模型，从课程介绍中提取结构化知识点
    """
    prompt = f"""
    【任务】请从课程介绍中智能识别其教学目标，并结构化提取知识点。

    【核心原则】
    1. 不要忽略“项目”“实战”“综合应用”等内容 —— 它们可能是课程的核心目标
    2. main_point 必须是对 sub_points 的合理概括（上位概念）
    3. sub_points 应体现课程实际覆盖的技术/能力点
    4. 保持术语一致性（如“if/for” → “流程控制”）

    【输出格式】
    {{
      "id": "{course_id}",
      "main_point": "课程的核心教学目标（可为项目名或知识领域）",
      "sub_points": ["子知识点1", "子知识点2", ...]
    }}

    【课程介绍】
    {introduce_text}

    现在请输出（只返回 JSON）：
    """

    for _ in range(3):  # 最多重试 3 次
        try:
            response = Generation.call(
                model=MODEL_NAME,
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.3
            )
            if response.status_code == 200:
                text = response.output['text'].strip()
                if DEBUG:
                    print(f"\n🔍 模型输入:\n{prompt}")
                    print(f"🧠 模型输出: {text}")

                # 鲁棒解析 JSON
                result = robust_parse_json(text)
                if result and isinstance(result, dict):
                    # 验证字段
                    if 'id' in result and 'main_point' in result and 'sub_points' in result:
                        if isinstance(result['sub_points'], list):
                            return result
                print(f"结构不匹配: {text}")
            else:
                print(f"API 错误: {response.code} {response.message}")

            time.sleep(1.5)

        except Exception as e:
            print(f"调用异常: {e}")
            time.sleep(2)

    # 失败兜底
    return {
        "id": course_id,
        "main_point": "未知课程",
        "sub_points": []
    }

if __name__ == "__main__":
    print("正在加载 course_introduce.txt...")

    courses = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or ':' not in line:
                continue
            try:
                course_id_str, intro = line.split(':', 1)
                course_id = course_id_str.strip()
                courses.append((course_id, intro.strip()))
            except Exception as e:
                print(f"解析失败: {line} -> {e}")

    print(f"加载了 {len(courses)} 门课程")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for course_id, intro in tqdm(courses, desc="智能提取知识点"):
            result = extract_knowledge_from_introduce(course_id, intro)
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
            f.flush()

    print(f"\n完成！结构化知识点已保存至 '{OUTPUT_FILE}'")