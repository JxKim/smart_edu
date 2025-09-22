import pandas as pd
import pymysql

conn = pymysql.connect(host="localhost", user="root", password="123456", database="ai_edu")
sql = "SELECT id, name, age, city FROM user_info"
df = pd.read_sql(sql, conn)
df.to_csv("train_data.csv", index=False, encoding="utf-8")
