# your_main_script.py

import os
import pandas as pd
import mysql.connector
import json
import re
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from tenacity import retry, stop_after_attempt, wait_random_exponential
from tqdm import tqdm

from configuration.config import MYSQL_CONFIG
proxy_url = "http://127.0.0.1:10808"
os.environ['HTTP_PROXY'] = proxy_url
os.environ['HTTPS_PROXY'] = proxy_url
load_dotenv()
os.environ['GOOGLE_API_KEY'] = os.getenv("GOOGLE_API_KEY")
# --- 数据库函数 (保持不变) ---
def get_course_introductions():
    query = "SELECT id, course_name, course_introduce FROM course_info WHERE course_introduce IS NOT NULL AND course_introduce != '' AND (deleted IS NULL OR deleted != '1')"
    try:
        # 直接使用导入的 MYSQL_CONFIG
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        df = pd.read_sql(query, conn)
        print(f"成功从MySQL获取 {len(df)} 条课程介绍。")
        return df
    except mysql.connector.Error as err:
        print(f"数据库连接或查询错误: {err}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def extract_tags_with_llm(text: str) -> list:
    system_prompt = """
        你是一位资深的技术教育内容分析师。你的任务是通读一份课程介绍，并从中提取出最核心、最有价值的技术关键词、技能点和工具。

        # 任务
        从用户提供的课程介绍文本中，提取出关键的标签。

        # 提取规则
        1.  **分类提取**: 优先提取以下几类信息：
            - **核心技术/框架**: 如 Spring Cloud, Docker, Kubernetes, Vue.js, PyTorch。
            - **编程语言/概念**: 如 Java, Python, C语言, 面向对象, JVM内存结构。
            - **工具/平台**: 如 Git, Maven, Jenkins, Nginx, Linux。
            - **关键技能/能力**: 如 性能调优, 故障排查, 自动化部署, 数据分析, 架构设计。
        2.  **保持简洁和标准化**:
            - 标签应该是名词或动名词短语，例如 "微服务架构", "容器化技术", "数据可视化"。
            - 使用业界公认的、最通用的名称。例如，使用 "Kubernetes" 而不是 "k8s"。
        3.  **忽略无关内容**:
            - 过滤掉所有营销性、引导性、评价性的词语。例如，忽略 "从零到一", "高薪必备", "轻松上手", "打下坚实基础"。
            - 忽略纯粹的课程结构描述，如 "第一阶段", "通过本项目学习"。
        4.  **数量灵活**: 根据文本内容的丰富程度，提取 5 到 15 个最核心的标签。如果内容确实很少，少于5个也可以。
        5.  **去重与合并**: 如果多个词语描述同一个意思，选择最核心的那个。例如，文本中提到 "类、对象、继承、多态"，可以提取一个更概括的标签 "面向对象编程"。

        # 输出格式
        必须返回一个只包含 "tags" 键的 JSON 对象，其值为一个字符串数组。
        格式示例:
        {"tags": ["标签1", "标签2", "标签3"]}
        """
    try:
        model = init_chat_model(
            model='gemini-2.5-flash',
            model_provider="google_genai",
            temperature=0.5
        )
        response = model.invoke(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            response_format={"type": "json_object"},
        )
        # **关键修复**: 直接访问 .content 属性
        result_str = response.content
        match = re.search(r'\{.*\}', result_str, re.DOTALL)
        if match:
            result_str = match.group(0)
        print(result_str)
        result_json = json.loads(result_str)
        tags = result_json.get("tags", [])
        return tags if isinstance(tags, list) else []
    except Exception as e:
        print(f"处理文本时出错: {text[:50]}... \n错误: {e}")
        return []

def process_courses():
    courses_df = get_course_introductions()
    if courses_df.empty:
        print("没有获取到数据，程序退出。")
        return

    tqdm.pandas(desc="提取知识点标签")
    print("开始使用LLM提取知识点标签...")
    courses_df['knowledge_tags'] = courses_df['course_introduce'].progress_apply(extract_tags_with_llm)
    print("知识点标签提取完成。")

    output_file = 'course_knowledge_tags.csv'
    courses_df['tags_str'] = courses_df['knowledge_tags'].apply(lambda x: ','.join(x))
    courses_df[['id', 'course_name', 'tags_str', 'knowledge_tags']].to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"处理结果已保存到: {output_file}")


if __name__ == "__main__":
    process_courses()