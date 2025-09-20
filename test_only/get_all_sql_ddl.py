import pymysql
from pymysql.cursors import DictCursor

connection = pymysql.connect(host='localhost',user='root',password='123456',port=3306)
cursor = connection.cursor()
cursor.execute('''
    select t.TABLE_NAME
    from information_schema.TABLES as t
    where t.TABLE_SCHEMA = 'ai_edu'
    ''')

results = cursor.fetchall()

for table in results:
    table_name = table[0]
    cursor.execute(f'show create table ai_edu.{table_name}')
    # import pdb;pdb.set_trace()
    print(cursor.fetchall()[0][1])

#cursor.fetchall()
#(('base_category_info', "CREATE TABLE `base_category_info` (\n  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',\n  `category_name` varchar(200) DEFAULT NULL COMMENT '分类名称',\n  `create_time` datetime DEFAULT NULL COMMENT '创建时间',\n  `update_time` datetime DEFAULT NULL COMMENT '更新时间',\n  `deleted` varchar(2) DEFAULT NULL COMMENT '是否删除',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb3 COMMENT='分类'"),)

