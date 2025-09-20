# export_chapters.py

import pymysql

# 🔧 数据库连接配置（请根据你的实际情况修改）
config = {
    'host': 'localhost',          # 数据库地址
    'user': 'root',               # 用户名
    'password': '17751817',  # 密码
    'database': 'edu_ai',    # 数据库名
    'charset': 'utf8mb4',         # 支持中文
    'cursorclass': pymysql.cursors.DictCursor  # 返回字典格式
}

# 📝 输出文件路径
output_file = 'chapter_name.txt'

try:
    # 建立数据库连接
    connection = pymysql.connect(**config)
    with connection.cursor() as cursor:
        # 执行查询
        sql = "SELECT id, course_id, chapter_name FROM chapter_info ORDER BY course_id, id"
        cursor.execute(sql)
        chapters = cursor.fetchall()  # 获取所有结果

    # 写入文本文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in chapters:
            line = f"{row['id']} {row['course_id']}: {row['chapter_name']}\n"
            f.write(line)

    print(f"✅ 成功从数据库读取 {len(chapters)} 条章节数据，并保存至 '{output_file}'")

except Exception as e:
    print(f"❌ 操作失败：{e}")

finally:
    if 'connection' in locals():
        connection.close()