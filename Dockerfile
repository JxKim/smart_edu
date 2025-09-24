FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 设置非 root 用户（安全最佳实践）
RUN adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser .
# 复制依赖文件并安装（利用 Docker 缓存优化构建速度）
RUN mkdir src
COPY --chown=appuser:appuser src ./src

# 设置目录权限
RUN chown -R appuser:appuser /app
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY .env .

# 容器内端口占用
EXPOSE 8000

# 启动命令（使用 Uvicorn 运行 FastAPI 应用）
CMD ["python", "src/web/app.py"]
#CMD ["sleep", "5m"]

