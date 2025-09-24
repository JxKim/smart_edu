import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from schema import Message
from service import ChatService

app = FastAPI()
# 当有人去访问我后端服务器的static路径的时候,把我static下面的资源全部给到他
app.mount("/static", StaticFiles(directory="static"), name="static")

chat=ChatService()

# 当前端页面访问了http://127.0.0.1:8001/ 的时候，自动跳转到http://127.0.0.1:8001/static/index.html
@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")

@app.post("/api/chat")
def chat_service(request_data: Message):
    message=request_data.message
    response=chat.chat(message)
    return {"message":response.content}

if __name__ == "__main__":
    # 调用 uvicorn.run() 启动应用
    uvicorn.run(
        app="app:app",  # 格式：文件名:应用实例名（这里文件是 app.py，实例是 app）
        host="127.0.0.1",  # 绑定的 IP 地址，0.0.0.0 允许外部访问
        port=8001,  # 端口号
        reload=True  # 开发模式：代码修改后自动重启（生产环境需关闭）
    )