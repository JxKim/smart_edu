import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
src_root=Path(__file__).parent.parent
sys.path.append(str(src_root))

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse,RedirectResponse

from src.configuration import config
from src.web.schema import Question, Answer
from src.web.server import ChatServer


app=FastAPI()

app.mount(
    path="/static",  # 访问路径前缀
    app=StaticFiles(directory=config.WEB_STATIC_DIR),  # 本地文件夹路径（相对于当前文件）
    name="static"  # 可选名称
)
server=ChatServer()
@app.get("/")
def root():
    return FileResponse(config.WEB_STATIC_DIR/'index.html')

@app.post("/api/chat")
def serve(question:Question):
    answer=server.chat(question.message)
    return Answer(message=answer)

if __name__=="__main__":
    uvicorn.run('web.app:app', host="0.0.0.0", port=9000)


