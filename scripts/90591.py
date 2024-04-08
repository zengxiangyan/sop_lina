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
            SELECT source,shop_type,"品牌合并","贸易形式","sp人群" ,"sp中西药属性","sp剂型","sp分类","月",sum(sales)/100 AS sales_value,sum(num) AS sales_volume FROM(
                    select source,shop_type,toStartOfMonth(pkey) AS "月",year(pkey) AS "年",
                    IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], '海外', '国内')  AS "贸易形式",alias_all_bid,          
                    case 
                        when alias_all_bid in ('112244','1856905') then 'BY－HEALTH/汤臣倍健'
                        when alias_all_bid in ('53021','4834144') then 'Caltrate/钙尔奇'
                        when alias_all_bid in ('110054','4713434') then '朗迪'
                        when alias_all_bid in ('4780201','3456983') then '扶娃'
                        when alias_all_bid in ('4834116') then '澳诺金辛金丐特'
                        else dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') end AS "品牌合并","sp分类", "sp是否女性专用","sp人群" ,"sp中西药属性","sp剂型",sales,num,
                    case 
                        when "品牌合并"='Others' then 0 else 1 end as "排序字段"
                    from sop_e.entity_prod_90591_E  
                    {where})
            group by source,shop_type,"品牌合并","贸易形式","sp人群" ,"sp中西药属性","sp剂型","sp分类","月","排序字段"
            order by "排序字段" desc,sales_value desc;                
    """.format(where=where)
    sql = """
        select source,shop_type,
        case
            when alias_all_bid in ('112244','1856905') then 'BY－HEALTH/汤臣倍健'--汤臣倍健 &健力多
            when alias_all_bid in ('53021','4834144') then 'Caltrate/钙尔奇'--钙尔奇&金钙尔奇
            when alias_all_bid in ('110054','4713434') then  '朗迪'--朗迪&精朗迪
            when alias_all_bid in ('4780201','3456983') then  '扶娃' --“扶娃”需要合并“扶娃”和“新盖金典”两个品牌
            when alias_all_bid in ('4834116') then  '澳诺金辛金丐特'--自定义品牌名称 澳诺金辛金丐特
            when alias_all_bid in ('5950105') then 'witsBB/健敏思'
            when alias_all_bid in ('112244','131009','53021','387336','401951','326236','59590','5608679','273629','4713434','3847776','48678',
            '59539','5904358','4834116','5950105','380617','52323','819397','474001','4780201','474178','1856905','4834144','110054','3456983') then dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '')
            else 'Others' end AS "品牌",
            IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], '海外', '国内')  AS "贸易形式","sp人群" AS "人群", "sp中西药属性" AS "药保属性","sp剂型" AS "剂型","sp分类" AS "分类",
                    toStartOfMonth(pkey) AS "月",
               sum(sales)/100 AS sales_value,
               sum(num) AS sales_volume,
               case when "品牌"='Others' then 0 else 1 end as "排序字段"
        from sop_e.entity_prod_90591_E
        {where}
        group by source,shop_type,"品牌","贸易形式","sp人群", "sp中西药属性","sp剂型","sp分类","月"
        order by "排序字段" desc,sales_value desc
    """.format(where=where)
    return sql

def get_data(db,sql):

    data = db.query_all(sql,as_dict=True)
    return data


def main():
    db = connect_clickhouse('chsop')
    where = """where pkey>='2023-12-01' and pkey<'2024-01-01' and "sp子品类"='钙'"""
    json = {
        'start_date': '2023-12-01',
        'end_date': '2024-01-01',
        'eid': '90591',
        'table': 'entity_prod_90591_E',
        'cid': [],
    }
    sql = get_sql_info(where=where)
    data = get_data(db,sql)
    print(len(data))
    ################################################################################################################
    # #根据sql取出来的source和shop_type获取对应平台的中文名称
    data = source_cn(data)
    ################################################################################################################

    ################################################################################################################
    # #根据sql取出来的source和shop_type获取对应平台的英文名称
    # data = source_en(data)
    ################################################################################################################

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
    data = data.groupby(["平台","品牌","贸易形式","人群" ,"药保属性","剂型","分类","月"],as_index=False)[['sales_value','sales_volume']].sum()
    ################################################################################################################

    ################################################################################################################
    # #输出保存最终文件为csv到report文件夹下
    output = r'..\report\钙制剂 key brand +others 属性分类汇总_.csv'
    data.to_csv(output,index=False,encoding='utf-8-sig')

    # 需要保存为xlsx
    with open(r'..\report\钙制剂 key brand +others 属性分类汇总_.xlsx', 'w', encoding='utf-8-sig',newline='') as f:
        csv_w = csv.writer(f)
        csv_w.writerow(['平台', 'alias_pid', '时间'])
        csv_w.writerows(numpy(data))
    # with pd.ExcelWriter(r'..\report\钙制剂 key brand +others 属性分类汇总_.xlsx', engine='xlsxwriter',options={'strings_to_urls': False, 'constant_memory': False}) as writer:
    #     data.to_excel(writer, sheet_name='属性分类汇总.xlsx', float_format='%.2f', index=False)
    ################################################################################################################

if __name__ == '__main__':
    main()
