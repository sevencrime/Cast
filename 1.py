# -*- coding: utf-8 -*-
import psycopg2
# 获得连接
conn = psycopg2.connect(database='vap',user='postgres',password='Buzhongyao123',host='postgres.develop.meetwhale.com',port='5432')
# 获得游标对象
cursor = conn.cursor()
# sql语句
sql = """
SELECT SUM(gmv)
FROM vap_channel_analysis
WHERE douyin_no = 'wusu868688'
    AND etl_date >= to_char(to_timestamp(1678896000), 'YYYY-MM-DD')
    AND etl_date <= to_char(to_timestamp(1679241599), 'YYYY-MM-DD')

"""
# 执行语句
cursor.execute(sql)
# 获取单条数据.
data = cursor.fetchall()
# 打印
for x in data:
    print(x)

# 事物提交
conn.commit()
# 关闭数据库连接
conn.close()
