# -*- coding: utf-8 -*-
import pandas as pd
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pandas as pd
from common import cid_name

conf = {
    "user": "yinglina",
    "password": "xfUW5GMr",
    "server_host": "10.21.90.15",
    "port": "10081",
    "db": "sop"
}
connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
# sql = 'select * from sop_e.entity_prod_92087_E limit 1,1000'
fuctions = ['Nourish & repair','Anti-Hair Loss','Volumizing','Oil control','Color control','Anti-dandruff','Smooth & hydration','men care']
for num,fuction in enumerate(fuctions):
    sql = f"""
SELECT `sp功效-{fuction}` as `功效`,toStartOfMonth(pkey)as `月份`,
case when toStartOfMonth(pkey) >= '2021-01-01' AND toStartOfMonth(pkey) < '2021-04-01' then '21Q1'
     when toStartOfMonth(pkey) >= '2021-04-01' AND toStartOfMonth(pkey) < '2021-07-01' then '21Q2'
     when toStartOfMonth(pkey) >= '2021-07-01' AND toStartOfMonth(pkey) < '2021-10-01' then '21Q3'  
     when toStartOfMonth(pkey) >= '2021-10-01' AND toStartOfMonth(pkey) < '2022-01-01' then '21Q4'
     when toStartOfMonth(pkey) >= '2022-01-01' AND toStartOfMonth(pkey) < '2022-04-01' then '22Q1'
     when toStartOfMonth(pkey) >= '2022-04-01' AND toStartOfMonth(pkey) < '2022-07-01' then '22Q2'
     when toStartOfMonth(pkey) >= '2022-07-01' AND toStartOfMonth(pkey) < '2022-10-01' then '22Q3'  
     when toStartOfMonth(pkey) >= '2022-10-01' AND toStartOfMonth(pkey) < '2023-01-01' then '22Q4'
     when toStartOfMonth(pkey) >= '2023-01-01' AND toStartOfMonth(pkey) < '2023-04-01' then '23Q1'
     when toStartOfMonth(pkey) >= '2023-04-01' AND toStartOfMonth(pkey) < '2023-07-01' then '23Q2'
     when toStartOfMonth(pkey) >= '2023-07-01' AND toStartOfMonth(pkey) < '2023-10-01' then '23Q3'  
     when toStartOfMonth(pkey) >= '2023-10-01' AND toStartOfMonth(pkey) < '2024-01-01' then '23Q4'
else '其他' end as season,SUM(sales)/100000000 FROM sop_e.entity_prod_92111_E 
WHERE ((`date`>='2023-01-01' AND `date`<'2023-10-01')
AND (`sp子品类` in ['美发护发'])
AND (`sp功效-{fuction}`!='否'))
GROUP BY `月份`,season,`功效`
"""
    sql = f"""
select cid,`sp功效-{fuction}` as `功效`,alias_all_bid,item_id,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as Brand,`功效`,argMax(name,num)name,argMax(img,num) as `图片`,
case 
	when source = 1 and shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',item_id)
	when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
	when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
	when source = 5 then CONCAT('https://goods.kaola.com/product/',item_id,'.html')
	when source = 6 then CONCAT('https://product.suning.com/',item_id,'.html')
	when source = 9 then CONCAT('www.jiuxian.com/goods.',item_id,'.html')
	else '其他' end as url,sum(sales)/100 as `销售额` ,toStartOfMonth(pkey) as `月份`
from sop_e.entity_prod_92111_E
where date>='2023-04-01' and date<'2024-04-01' 
and `sp子品类` in ['美发护发'] 
and `sp功效-{fuction}`!='否'
group by url,`月份`,cid,alias_all_bid,item_id,`功效`
order by `销售额` desc"""

    print(num)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
    finally:
        cursor.close()
        session.close()
    if num == 0:
        newdata = mydata
    else:
        newdata = pd.concat([newdata, mydata], axis=0, ignore_index=True)

data = newdata
json = {
        'start_date': '2023-01-01',
        'end_date': '2024-04-01',
        'eid': '92111',
        'table': 'entity_prod_92111_E',
        'cid': [],
    }
# data = cid_name(data,json)
# data['子品类'] = [ i.split('>>')[len(i.split('>>'))-1] for i in data['Full_path']]
# data = data[['子品类', '功效', 'alias_all_bid', 'item_id', 'Brand', 'argMax(name, num)','图片', 'url', '销售额', '月份']]
lv_name = pd.read_csv(r'C:\Users\zeng.xiangyan\Downloads\92111 (2).csv')[['cid','lv1name','lv2name','lv3name','lv4name','lv5name']]
data = pd.merge(data, lv_name, on=['cid'], how='left', suffixes=('_history', '_new'))
data.to_csv(r'C:\Users\zeng.xiangyan\Downloads\雅诗兰黛23年Q2-24年Q1rowdata.csv',index=False,encoding='utf-8-sig')