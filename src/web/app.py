import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from configuration.config import WEB_STATIC_DIR
from web.schemas import Question, Answer
from web.server import ChatService

app = FastAPI()

app.mount("/static", StaticFiles(directory=WEB_STATIC_DIR), name="static")

service = ChatService()
@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")
@app.post("/api/chat")
def read_item(question:Question):
    print(question.message)
    result = service.chat(question.message)
    return Answer(message=result)
    # return Answer(message=question.message)

if __name__ == "__main__":
    uvicorn.run('web.app:app', host="localhost", port=9000)