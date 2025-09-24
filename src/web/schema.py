from openai import BaseModel

# 这里定义的意义
# 1)前端传递过来的json字符串,其实是一个匿名对象，这里的Message就是给了匿名对象一个类型
# 同时不做额外配置,就是要求这个对象,必须有message属性,并且必须是str
# 2）当接口的代码中使用注解之后,会自动把前端传过来的符合要求的json字符串，转换为这个类型的对象
# 这样就可以用对象实例（对象实例的名字，就是你给的变量的名字）.的形式 拿到前端请求后端时候的值了
class Message(BaseModel):
    message:str