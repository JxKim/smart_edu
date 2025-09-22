import html
import re

from bs4 import BeautifulSoup

from configuration.config import RAW_DIR
from datasync.utils import MysqlUtil

mysql_reader = MysqlUtil()

course_descriptions_sql = """
    select course_introduce
    from course_info
"""
chapters_sql = """
    select chapter_name
    from chapter_info
"""
questions_sql = """
    select question_txt
    from ai_edu.test_question_info
"""

course_descriptions = mysql_reader.read(course_descriptions_sql)
course_descriptions = [description['course_introduce'] for description in course_descriptions]


chapters_introduce = mysql_reader.read(chapters_sql)
chapters_introduce = [chapter['chapter_name'] for chapter in chapters_introduce]

questions_html = mysql_reader.read(questions_sql)

soups = [BeautifulSoup(question['question_txt'], "html.parser") for question in questions_html ]
questions_txt = []
for soup in soups:
    for tag in soup(["style", "script"]):
        tag.decompose()
    parts = [html.unescape(p.get_text(strip=True)) for p in soup.find_all("p")]
    if len(parts) > 5:
        text = "\n".join(parts)
        text = re.sub(r'\s*(NBSP|ZWNBSP)\s*', ' ', text)
        text = text.replace("\uFEFF", "")  # 去掉 BOM / 零宽不换行空格
        text = text.replace("\u200B", "")# 去掉零宽空格
        text = re.sub(r'\s+', ' ', text).strip()
        questions_txt.append(text)



data_txt = course_descriptions + chapters_introduce + questions_txt
with open(str(RAW_DIR /"data.txt"), "w", encoding="utf-8") as f:
    for line in data_txt:
        f.write(line)
        f.write("\n")