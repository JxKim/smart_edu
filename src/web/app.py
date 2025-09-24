import uvicorn  # 用于启动 FastAPI 服务（提供 ASGI 服务器）
from fastapi import FastAPI  # FastAPI 框架核心类
from starlette.responses import RedirectResponse  # 用于做 HTTP 重定向
from starlette.staticfiles import StaticFiles  # 用于挂载静态文件目录

from configuration import config  # 配置文件，包含静态文件路径等
from web.schemas import Question, Answer  # Pydantic 数据模型，用于请求和响应验证
from web.server import ChatService  # 业务逻辑类，负责处理聊天逻辑

# 创建 FastAPI 应用对象
app = FastAPI()

# 挂载静态文件目录
# 访问 /static/... 时，直接从 config.WEB_STATIC_DIR 指定的目录中返回文件
# 例如 /static/index.html 会返回本地 WEB_STATIC_DIR/index.html
app.mount("/static", StaticFiles(directory=config.WEB_STATIC_DIR), name="static")

# 实例化聊天服务对象
# 为什么单例：避免每次请求都重新创建对象，节省资源
service = ChatService()

# 根路由 GET /
# 当用户访问 http://localhost:8000/ 时，直接重定向到前端首页 index.html
@app.get("/")
def read_root():
    # RedirectResponse 会发送 302 重定向响应
    return RedirectResponse("/static/index.html")


# 聊天接口 POST /api/chat
# question: Question 表示请求体 JSON 必须有 message 字段，类型为 str
# 返回类型 Answer，FastAPI 会自动把 Pydantic 模型转为 JSON
@app.post("/api/chat")
def read_item(question: Question) -> Answer:
    # 调用 ChatService 的 chat 方法处理问题
    result = service.chat(question.message)
    # 返回 Answer 对象，FastAPI 自动序列化为 JSON
    return Answer(message=result)


# 脚本直接运行时启动服务
# uvicorn.run 指定 'web.app:app' 表示模块路径:应用对象
# host="0.0.0.0" 允许局域网其他设备访问
# port=8000 指定服务端口
if __name__ == '__main__':
    uvicorn.run('web.app:app', host="127.0.0.1", port=8000)
