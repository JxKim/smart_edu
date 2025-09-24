import sys

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles
from pathlib import Path
SRC_PATH=Path(__file__).parent.parent
sys.path.append(str(SRC_PATH))
from conf import config
from web.schemas import Question, Answer
from web.service import ChatService
import logging
logging.basicConfig(level=logging.DEBUG)
app = FastAPI()

app.mount("/static", StaticFiles(directory=config.WEB_STATIC_DIR), name="static")

service = ChatService()


@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")


@app.post("/api/chat")
def read_item(question: Question) -> Answer:
    result = service.chat(question.message)
    return Answer(message=result)


if __name__ == '__main__':
    uvicorn.run('web.app:app', host="0.0.0.0", port=8000)
