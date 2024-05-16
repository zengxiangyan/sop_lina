# -*- coding: utf-8 -*-
from common import source_cn,source_en,source_link,cid_name
from db import connect_clickhouse
from os.path import abspath, join, dirname
import sys
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
report_path = r'{path}'.format(path=sys.path[0])

def get_sql_info(where):

    sql = """
            SELECT source,shop_type,cid,"月","年",'' "平台","跨境",alias_all_bid,"品牌","品牌合并", "sp分类","sp是否女性专用", "sp人群" ,"sp中西药属性","sp剂型","店铺分类",sum(sales)/100 AS sales_value,sum(num) AS sales_volume, sum(sales)/100/sum(num) as Price_perUnit FROM(
                SELECT a.*,b."店铺分类" "店铺分类" from(
                    select source,shop_type,cid,sid,toStartOfMonth(pkey) AS "月",
                           year(pkey) AS "年",
                    IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], '海外', '国内')  AS "跨境",alias_all_bid,
                    dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') AS "品牌",           
                    case 
                        when alias_all_bid in ('112244','1856905') then 'BY－HEALTH/汤臣倍健'
                        when alias_all_bid in ('53021','4834144') then 'Caltrate/钙尔奇'
                        when alias_all_bid in ('110054','4713434') then '朗迪'
                        when alias_all_bid in ('4780201','3456983') then '扶娃'
                        when alias_all_bid in ('4834116') then '澳诺金辛金丐特'
                        else "品牌"  end AS "品牌合并","sp分类", "sp是否女性专用","sp人群" ,"sp中西药属性","sp剂型",sales,num
                    from sop_e.entity_prod_90591_E  
                    {where})a
                LEFT JOIN (
                    WITH transform(source_origin, ['tb', 'tmall', 'jd', 'gome', 'jumei', 'kaola', 'suning', 'vip', 'pdd', 'jx', 'tuhu', 'dy', 'cdf', 'lvgou', ''], [1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 999], 0) AS source,
                        IF(chtype_h > 0, chtype_h, chtype_m) AS ch_type,
                        transform(ch_type, [1, 2, 3, 4], ['FSS', 'EKA', 'EDT', 'EKA_FSS'], toString(ch_type)) AS `店铺分类`
                    SELECT source, sid, `店铺分类` FROM mysql('192.168.30.93', 'graph', 'ecshop', 'cleanAdmin', '6DiloKlm')
                    WHERE `店铺分类` != '0')b
                ON toString(a.source)=toString(b.source) and toString(a.sid)=toString(b.sid))
            group by source,shop_type,cid,"月","年","跨境",alias_all_bid,"品牌","品牌合并", "sp分类","sp是否女性专用", "sp人群" ,"sp中西药属性","sp剂型","店铺分类";
            """.format(where=where)
    return sql

def get_data(db,sql):

    data = db.query_all(sql,as_dict=True)
    return data


def main():
    db = connect_clickhouse('chsop')
    where = """where pkey>='2020-01-01' and pkey<'2024-01-01' and "sp子品类"='钙'"""
    json = {
        'start_date': '2020-01-01',
        'end_date': '2024-01-01',
        'eid': '90591',
        'table': 'entity_prod_90591_E',
        'cid': [],
    }
    sql = get_sql_info(where=where)
    data = get_data(db,sql)

    ################################################################################################################
    # #根据sql取出来的source和shop_type获取对应平台的中文名称
    data = source_cn(data)
    ################################################################################################################

    ################################################################################################################
    # #根据sql取出来的source和shop_type获取对应平台的英文名称
    # data = source_en(data)
    ###############################################################################################################

    ################################################################################################################
    # #根据sql取出来的source、shop_type及item_id获取对应平台的宝贝链接
    # data = source_link(data)
    ################################################################################################################

    ################################################################################################################
    # #根据sql取出来的source、shop_type及cid获取对应平台的类目名称
    data = cid_name(data,json)
    ################################################################################################################

    ################################################################################################################
    # #删除最终数据中的source和shop_type列
    final_cols = [c for c in data.columns if c not in ['source', 'shop_type']]
    data = data[final_cols]

    ################################################################################################################

    ################################################################################################################
    # #输出保存最终文件为csv到report文件夹下
    output = r'..\report\test.csv'
    data.to_csv(output,index=False,encoding='utf-8-sig')
    ################################################################################################################

    ##test

if __name__ == '__main__':
    main()
