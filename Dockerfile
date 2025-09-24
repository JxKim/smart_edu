# 使用官方 Python 运行时作为基础镜像
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 设置非 root 用户（安全最佳实践）
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

# 复制依赖文件并安装（利用 Docker 缓存优化构建速度）
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY --chown=appuser:appuser . .

# 容器内端口占用
EXPOSE 8000

# 启动命令（使用 Uvicorn 运行 FastAPI 应用）
CMD ["python", "src/web/app.py"]
