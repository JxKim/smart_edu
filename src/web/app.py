import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles
from configuration.config import *
from web.schema import Question, Answer
from web.service import ChatService

app = FastAPI()
app.mount('/static', StaticFiles(directory=WEB_STATIC_DIR), name='static')

chatService = ChatService()

@app.get('/')
def go_root():
    return RedirectResponse('/static/index.html')

@app.post('/api/chat')
def go_item(question: Question)-> Answer:
    answer = chatService.chat(question.message)
    return Answer(message=answer)

if __name__ == '__main__':
    uvicorn.run('web.app:app', host='0.0.0.0', port=8888)