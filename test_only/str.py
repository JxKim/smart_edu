x = '我喜欢English'

print(x.lower())


x2 = '我 喜 欢English h a'

print(x2.lower().replace(' ', ''))
# x2_lower = x2.lower()
# print(x2_lower)
# import pdb;pdb.set_trace()
# print(list(x2))

print('-'*50)
text = '讲解ApacheSpark分布式计算框架的使用,包括RDD、DataFrame、SQL查询、流式处理、任务调度等。'
print(list(text))
print(list(text.lower()))