import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse,RedirectResponse

from configuration import config
from web.schema import Question, Answer
from web.server import ChatServer

app=FastAPI()

app.mount(
    path="/static",  # 访问路径前缀
    app=StaticFiles(directory=config.WEB_STATIC_DIR),  # 本地文件夹路径（相对于当前文件）
    name="static"  # 可选名称
)
server=ChatServer()
@app.get("/")
def root():
    return RedirectResponse('/static/index.html')

@app.post("/api/chat")
def serve(question:Question):
    answer=server.chat(question.message)
    return Answer(message=answer)

if __name__=="__main__":
    uvicorn.run('web.app:app', host="127.0.0.1", port=9000)


