import pandas

from db import connect_clickhouse
from os.path import abspath, join, dirname
import sys
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.styles import Font, Fill, Border, Alignment, Protection
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
report_path = r'{path}'.format(path=sys.path[0])
def get_sql_info(category,platform,brand,tbl,where):
    if platform in ['京东','京东-','考拉']:
        platform = "'{platform}'".format(platform=platform)
    sql = """
            select '{category}' "子品类"
            ,{platform} Platform
            ,gMonth() "时间"
            ,{brand} "品牌"
            ,sales_total/100000 "销售额"
            from {tbl}
            {where}
            group by "子品类",Platform,"时间","品牌"
""".format(category=category,platform=platform,brand=brand,tbl=tbl,where=where)
    return sql

def run():
    rule_dict = {
    'rule1':{"子品类":"纸尿裤",
             "tbl":"ali.trade",
             "品牌":"multiIf( rbid in(81393),'a 花王/Merries',rbid in(115561),'b 帮宝适/Pampers',rbid in(115557),'c 大王/GOO.N',rbid in(23517),'c 尤妮佳/Moony',rbid in(81415),'d 好奇/Huggies',rbid in(106143,614646,624250,762445),'e 碧芭宝贝/Beaba',rbid in(11709),'f 妈咪宝贝/Mamypoko',rbid in(24559,317587,615724),'g Babycare',rbid in(257536),'h 露安适/LELCH',rbid in (2170,20747,248895),'i 巴布豆/Bobdog',rbid in (85179),'j 宜婴/Eababy',rbid in (11162),'k 安儿乐/Anerle',rbid in (106205,637197,827069),'l 泰迪熊/Teddy Bear',rbid in (81376),'m 雀氏/Chiaus','Z 其它')",
             "platform":"""gPlatformIf('ali','淘宝','tmall_nomx','天猫','Z 其它')""",
             "where":f"""
            where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(50012227,50012531,50012712,50018813,201217001,201217101,201218407)
                and w_mid_ver(59111)
                and w_real_num(1)
                """
             }
    ,'rule2': {"子品类": "纸尿裤",
               "tbl": "jd.trade",
               "品牌": "multiIf( gAliasBid() in(48685,3780888),'a 花王/Merries',gAliasBid() in(274044),'b 帮宝适/Pampers',gAliasBid() in(67252),'c 大王/GOO.N',gAliasBid() in(97499,280406),'c 尤妮佳/Moony',gAliasBid() in(48751),'d 好奇/Huggies',gAliasBid() in(48663,5511797,5673494),'e 碧芭宝贝/Beaba',gAliasBid() in(206947),'f 妈咪宝贝/Mamypoko',gAliasBid() in(13067),'g Babycare',gAliasBid() in(280409,896538),'h 露安适/LELCH',gAliasBid() in (114153),'i 巴布豆/Bobdog',gAliasBid() in (347271),'j 宜婴/Eababy',gAliasBid() in (280006),'k 安儿乐/Anerle',gAliasBid() in (75759),'l 泰迪熊/Teddy Bear',gAliasBid() in (48731),'m 雀氏/Chiaus','Z 其它')",
               "platform":'京东',
                "where":f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(1546,7057,17309)
                and w_mid_ver(61271)
                """
            }
    , 'rule3': {"子品类": "纸尿裤",
                "tbl": "kaola.trade",
                "platform":'考拉',
                "品牌":"multiIf(gAliasBid() in(48685,3780888),'a 花王/Merries',gAliasBid() in(274044),'b 帮宝适/Pampers',gAliasBid() in(67252),'c 大王/GOO.N',gAliasBid() in(97499,280406),'c 尤妮佳/Moony',gAliasBid() in(48751),'d 好奇/Huggies',gAliasBid() in(48663),'e 碧芭宝贝/Beaba',gAliasBid() in(206947),'f 妈咪宝贝/Mamypoko',gAliasBid() in(13067),'g Babycare',gAliasBid() in(280409,6045396),'h 露安适/LELCH',gAliasBid() in (114153),'i 巴布豆/Bobdog',gAliasBid() in (347271),'j 宜婴/Eababy',gAliasBid() in (280006),'k 安儿乐/Anerle',gAliasBid() in (75759),'l 泰迪熊/Teddy Bear',gAliasBid() in (48731),'m 雀氏/Chiaus','Z 其它')",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(1498,114046,104181,1497)
                and w_mid_ver(60629)
                """
                }
    ,'rule4': {"子品类": "防晒",
               "tbl": "ali.trade",
               "platform":"""gPlatformIf('ali','淘宝','tmall_nomx','天猫','Z 其它')""",
               "品牌":"multiIf(rbid IN(47081, 81393),'a 碧柔/Biore', rbid IN(621275, 18011),'b 皑丽/Allie', rbid IN(243988),'c 珂润/Curel', rbid IN(61944),'d 苏菲娜/SOFINA', rbid IN(385248),'e 安热沙/ANESSA', rbid IN(2792),'f 资生堂/SHISEIDO', rbid IN(587490),'g 怡思丁/ISDIN', rbid IN(1964),'h 兰蔻/LANCOME', rbid IN(44719),'i 欧莱雅/L OREAL', rbid IN(1397),'j 玉兰油/OLAY', rbid IN(1354),'k 曼秀雷敦/MENTHOLATUM', rbid IN(107054),'l 薇诺娜/WINONA', rbid IN(360678),'m 黛珂/COSME DECORTE', rbid IN(316639),'n 蜜丝婷/MISTINE', rbid IN (567482, 622827),'s 莱斯璧/Recipe', 'Z 其它')",
              "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(50011982,201171302,201744402)
                and w_mid_ver(59111)
                and w_real_num(1)
                """
              }
    , 'rule5': {"子品类": "防晒",
                "tbl": "jd.trade",
                "platform": '京东',
                "品牌":"multiIf(gAliasBid() IN(53144, 48685),'a 碧柔/Biore', gAliasBid() IN(6491717, 6508446, 634624, 52028, 4846617),'b 皑丽/Allie', gAliasBid() IN(244970),'c 珂润/Curel', gAliasBid() IN(218453),'d 苏菲娜/SOFINA', gAliasBid() IN(1058246),'e 安热沙/ANESSA', gAliasBid() IN(51962),'f 资生堂/SHISEIDO', gAliasBid() IN(883874),'g 怡思丁/ISDIN', gAliasBid() IN(105167),'h 兰蔻/LANCOME', gAliasBid() IN(44785),'i 欧莱雅/L OREAL', gAliasBid() IN(53147),'j 玉兰油/OLAY', gAliasBid() IN(52143),'k 曼秀雷敦/MENTHOLATUM', gAliasBid() IN(244645),'l 薇诺娜/WINONA', gAliasBid() IN(218724),'m 黛珂/COSME DECORTE', gAliasBid() IN(1991823),'n 蜜丝婷/MISTINE', gAliasBid() IN (305706),'s 莱斯璧/Recipe', 'Z 其它')",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(13548)
                and w_mid_ver(61271)
                """
                }
    , 'rule6': {"子品类": "防晒",
                "tbl": "kaola.trade",
                "platform": '考拉',
                "品牌":"multiIf( gAliasBid() in(53144),'a 碧柔/Biore',gAliasBid() in(6491717,6508446,634624,52028,4846617),'b 皑丽/Allie',gAliasBid() in(244970),'c 珂润/Curel',gAliasBid() in(218453),'d 苏菲娜/SOFINA',gAliasBid() in(1058246),'e 安热沙/ANESSA',gAliasBid() in(51962),'f 资生堂/SHISEIDO',gAliasBid() in(883874),'g 怡思丁/ISDIN',gAliasBid() in(105167),'h 兰蔻/LANCOME',gAliasBid() in(44785),'i 欧莱雅/L OREAL',gAliasBid() in(53147),'j 玉兰油/OLAY',gAliasBid() in(52143),'k 曼秀雷敦/MENTHOLATUM',gAliasBid() in(244645),'l 薇诺娜/WINONA',gAliasBid() in(218724),'m 黛珂/COSME DECORTE',gAliasBid() in(1991823),'n 蜜丝婷/MISTINE',gAliasBid() in (305706),'s 莱斯璧/Recipe','Z 其它')",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                AND cidsIn(2112, 2113, 2116, 110043, 114039)
                and w_mid_ver(60629)
                """
                }
    , 'rule7': {"子品类": "卫生巾",
                "tbl": "ali.trade",
                "platform": """gPlatformIf('ali','淘宝','tmall_nomx','天猫','Z 其它')""",
                "品牌": "multiIf( rbid in(251020,81393,648484),'a 乐而雅/laurier',rbid in(82960),'b 苏菲/Sofy',rbid in(83030),'c 护舒宝/Whisper',rbid in(7813),'d ABC',rbid in(65266),'e 高洁丝/Kotex',rbid in(263533),'f 七度空间/Space7',rbid in(117575),'h 洁婷/Ladycare',rbid in (288404),'i 自由点/Freemore',rbid in (2069),'j 全棉時代/Purcotton',rbid in (104242),'k 洁伶',rbid in (821452),'l 她研社/Herlab','Z 其它')",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(50012475,50012476,127692025)
                and w_mid_ver(59111)
                and w_real_num(1)
                """
                }
    , 'rule8': {"子品类": "卫生巾",
                "tbl": "jd.trade",
                "platform": "京东",
                "品牌": "multiIf(gAliasBid() in(279510,48685),'a 乐而雅/laurier', gAliasBid() in(75672),'b 苏菲/Sofy', gAliasBid() in(254863),'c 护舒宝/Whisper', gAliasBid() in(11250),'d ABC', gAliasBid() in(279489),'e 高洁丝/Kotex', gAliasBid() in(279486),'f 七度空间/Space7', gAliasBid() in(279499),'h 洁婷/Ladycare',gAliasBid() in (279522),'i 自由点/Freemore',gAliasBid() in (68142),'j 全棉時代/Purcotton',gAliasBid() in (279497),'k 洁伶',gAliasBid() in (6650763),'l 她研社/Herlab','Z 其它')",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(16800,16802,16804,21489)
                and w_mid_ver(61271)
                """
                }
    , 'rule9': {"子品类": "卫生巾",
                "tbl": "jd.trade",
                "platform": "京东-",
                "品牌": "multiIf(gAliasBid() in(279510,48685),'a 乐而雅/laurier','Z 其它') ",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(16805)
                and w_mid_ver(61271)
                and gAliasBid() in(279510,48685)
                """
                }
    , 'rule10': {"子品类": "卫生巾",
                 "tbl": "kaola.trade",
                 "platform": "考拉",
                 "品牌": "multiIf( gAliasBid() in(279510,48685),'a 乐而雅/laurier',gAliasBid() in(75672,97499),'b 苏菲/Sofy',gAliasBid() in(254863),'c 护舒宝/Whisper',gAliasBid() in(11250),'d ABC',gAliasBid() in(279489),'e 高洁丝/Kotex',gAliasBid() in(279486),'f 七度空间/Space7',gAliasBid() in(279499),'h 洁婷/Ladycare',gAliasBid() in (279522),'i 自由点/Freemore',gAliasBid() in (68142),'j 全棉時代/Purcotton',gAliasBid() in (279497),'k 洁伶',gAliasBid() in (6650763),'l 她研社/Herlab','Z 其它') ",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(1128,1129)
                and w_mid_ver(60629)
                """
                }
    , 'rule11': {"子品类": "洗衣液",
                 "tbl": "ali.trade",
                 "platform": """gPlatformIf('ali','淘宝','tmall_nomx','天猫','Z 其它')""",
                 "品牌": "multiIf( rbid in(81393,395263,27168),'a 洁霸/Attack',rbid in(37063),'b 蓝月亮/Bluemoon',rbid in(2054),'c 威露士/Walch',rbid in(12612),'d 奥妙/OMO',rbid in(10014),'e 超能/Chaoneng',rbid in(94553),'f 立白/Liby',rbid in(18756),'g 汰渍/Tide',rbid in(19671),'h 碧浪/Ariel',rbid in(567491),'i 当妮/Downy',rbid in(334443),'j 好爸爸/Kispa',rbid in(784787,291541),'k 妈妈壹选/La Mamma','Z 其它') ",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(50018977,201169302,201171001,201395605,201513501)
                and w_mid_ver(59111)
                and w_real_num(1)
                """
                }
    , 'rule12': {"子品类": "洗衣液",
                 "tbl": "jd.trade",
                 "platform": "京东",
                 "品牌": "multiIf( gAliasBid() in(48685,104796),'a 洁霸/Attack',gAliasBid() in(19024),'b 蓝月亮/Bluemoon',gAliasBid() in(51946),'c 威露士/Walch',gAliasBid() in(150266),'d 奥妙/OMO',gAliasBid() in(184370),'e 超能/Chaoneng',gAliasBid() in(319425),'f 立白/Liby',gAliasBid() in(279434),'g 汰渍/Tide',gAliasBid() in(86061),'h 碧浪/Ariel',gAliasBid() in(802120,1195791),'i 当妮/Downy',gAliasBid() in(380377),'j 好爸爸/Kispa',gAliasBid() in(1384820),'k 妈妈壹选/La Mamma','Z 其它') ",
                "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(15924)
                and w_mid_ver(61271)
                """
                }
    , 'rule13': {"子品类": "洗衣液",
                 "tbl": "kaola.trade",
                 "platform":"考拉",
                 "品牌": "multiIf( gAliasBid() in(48685,104796),'a 洁霸/Attack',gAliasBid() in(19024),'b 蓝月亮/Bluemoon',gAliasBid() in(51946),'c 威露士/Walch',gAliasBid() in(5905747,150266),'d 奥妙/OMO',gAliasBid() in(184370),'e 超能/Chaoneng',gAliasBid() in(319425),'f 立白/Liby',gAliasBid() in(279434),'g 汰渍/Tide',gAliasBid() in(86061),'h 碧浪/Ariel',gAliasBid() in(802120),'i 当妮/Downy',gAliasBid() in(6453917,380377),'j 好爸爸/Kispa',gAliasBid() in(5917899),'k 妈妈壹选/La Mamma','Z 其它') ",
                 "where": f"""
             where memory_limit('10G')
                and w_start_date('{s_date}')
                AND w_end_date_exclude('{e_date}')
                and platformsIn('all')
                and cidsIn(2458,112238,112251)
                and w_mid_ver(60629)
                """
                 }
    }
    sql_union = '''
            union all'''
    sql = ''
    for rule in rule_dict.keys():
        sql_tmp = get_sql_info(rule_dict[rule]['子品类'],rule_dict[rule]['platform'],rule_dict[rule]['品牌'],rule_dict[rule]['tbl'],rule_dict[rule]['where'],)
        if rule=='rule1':
            sql = 'select * from(' + sql_tmp
        else:
            sql = sql + sql_union + sql_tmp
    sql += ' )order by "子品类",Platform,"品牌" '
    data = get_data(db,sql)
    return data

def get_data(db,sql,as_dict=True):

    data = db.query_all(sql,as_dict=as_dict)
    data = pandas.DataFrame(data)
    data.to_csv("huawang.csv",encoding='utf-8-sig',index=False)
    return data

if __name__ == "__main__":
    db = connect_clickhouse('chmaster2')
    s_date = '2024-05-01'
    e_date = '2024-06-01'
    run()

