import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles
from contextlib import asynccontextmanager

from configuration import config
from web.server import ChatService
from pydantic import BaseModel


class Question(BaseModel):
    message: str


class Answer(BaseModel):
    message: str

app_context = {}


# 2. 定义 lifespan 事件处理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- 应用启动时执行的代码 ---
    print(">>> Application starting up: Initializing ChatService...")
    # 在这里进行初始化，这部分代码只会由 Uvicorn worker 执行一次
    app_context["service"] = ChatService()
    print(">>> ChatService initialized successfully.")
    yield

    print(">>> Application shutting down...")
    app_context.clear()  # 清理资源

app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory=config.WEB_STATIC_DIR), name="static")

@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")


@app.post("/api/chat")
def read_item(question: Question) -> Answer:
    # 5. 从上下文中获取 service 实例
    service: ChatService = app_context["service"]
    result = service.chat(question.message)
    return Answer(message=result)


if __name__ == '__main__':
    # 注意：当使用 reload=True 时，Uvicorn 可能会有更复杂的行为，
    # 但对于生产环境或普通运行，lifespan 是标准做法。
    uvicorn.run('web.app:app', host="0.0.0.0", port=8888)