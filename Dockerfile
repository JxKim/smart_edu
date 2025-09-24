FROM python:3.12-slim

WORKDIR /app

# 复制生成的 requirements.txt
COPY requirements.txt .

# 安装所有依赖
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "src/web/app.py"]
