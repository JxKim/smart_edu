import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from config import config
from web.ChatService import ChatService

app=FastAPI()

app.mount("/static",StaticFiles(directory=config.WEB_STATIV_DIR),name="static")

service=ChatService()

class Question(BaseModel):
    message:str

class Answer(BaseModel):
    message:str


@app.get("/")
def read_root():
    return RedirectResponse("/static/index.html")

def read_item(question:Question):
    # result=service.chat()
    return Answer(message="")

if __name__=="__main__":
    uvicorn.run("web.app:app",host="0.0.0.0",port=5555)