import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

import configuration.config
from configuration import config
from web.schemas import Question, Answer

app = FastAPI()
app.mount('/static', StaticFiles(directory=config.STATIC_DIR), name='static')


class ChatService:
    def chat(self, question):
        return 'Hello World'


service = ChatService()


@app.get('/')
async def homepage():
    return RedirectResponse('/static/index.html')


@app.post('/chat')
async def chat(question: Question):
    result = service.chat(question.message)
    return Answer(message=result)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=9000)
