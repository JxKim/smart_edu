import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles
from schemas import Question, Answer

from configuration import config
from web.server import ChatService

app = FastAPI()

app.mount("/static", StaticFiles(directory=config.WEB_STATIC_DIR), name="static")
service = ChatService()
@app.get("/")
def root():
    return RedirectResponse(url="/static/index.html")

@app.post("/chat")
def read_item(question:Question)->Answer:
    result = service.chat(question.message)
    return Answer(message = result)
if __name__ == "__main__":
    uvicorn.run('web.app:app', host="0.0.0.0", port=8000)