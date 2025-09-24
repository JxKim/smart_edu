FROM python:3.12-slim-bookworm

WORKDIR /app

# 设置PyPI镜像源为国内镜像，提高下载速度
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

COPY ./checkpoints /app/checkpoints
COPY ./src ./src

COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

EXPOSE 8000

CMD ["python","src/web/app.py"]