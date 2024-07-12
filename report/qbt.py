# -*- coding: utf-8 -*-
from db import connect_clickhouse
from os.path import abspath, join, dirname
import sys

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
report_path = r'{path}'.format(path=sys.path[0])
db = connect_clickhouse('chmaster2')
print(db.query_all("""select '纸尿裤' "子品类"
	,gMonth() "时间"
	,gPlatformIf('ali','Merries淘宝总销售额','tmall_nomx','Merries天猫总销售额','Z 其它') Platform
	,gPlatformIf('tmall_hknomx','Merries天猫国际销售额','tmall_csnoxx','Merries天猫超市销售额','ali_qq','Merries全球购销售额','其它') Platform_sub
	,if(sid = 8817405,'Merries天猫官方旗舰店销售额','其它') shop
	,concat_ws('-',Platform,Platform_sub,shop) "sup"
	,sales_total/100000 "销售额"
	from ali.trade
	where memory_limit('10G')
		and w_start_date('2024-02-01')
		AND w_end_date_exclude('2024-03-01')
		and platformsIn('all')
		and cidsIn(50012227,50012531,50012712,50018813,201217001,201217101,201218407)
		and rbid in(81393)
		and w_mid_ver(59111)
		and w_real_num(1)
	group by "子品类",Platform,"时间",Platform_sub,shop"""))