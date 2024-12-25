# -*- coding: utf-8 -*-
from common import source_cn,source_en,source_link,cid_name
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

def get_top_brand(db,tbl,where):
    sql="""
        with
        case when source = 1 and shop_type < 20 and shop_type != 9 then 'tb'
             when source = 1 and shop_type > 20 or source = 1 and shop_type = 9 then 'tmall'
             when source = 2 then 'jd'
             when source = 5 then 'kaola'        
             when source = 6 then 'suning'        
             when source = 9 then 'jiuxian'
             when source = 11 then 'douyin'             
             else '其他' end as "平台",
        IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], 'Cross-border', 'Domestic')  AS "跨境"
        select alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') AS "品牌",
        sum(sales)/100 AS sales_value
        from {tbl}
        {where}
        group by alias_all_bid
        order by sales_value desc 
        limit 10  
        """.format(tbl=tbl,where=where)
    ret = get_data(db,sql,as_dict=False)
    ret = [r[0] for r in ret]
    return ret

def get_Players_report(db, tbl,brand_where, players_where):
    brand_in = get_top_brand(db, tbl, brand_where)
    sql = '''
            with
            case when source = 1 and shop_type < 20 and shop_type != 9 then 'tb'
                 when source = 1 and shop_type > 20 or source = 1 and shop_type = 9 then 'tmall'
                 when source = 2 then 'jd'
                 when source = 5 then 'kaola'        
                 when source = 6 then 'suning'        
                 when source = 9 then 'jiuxian'
                 when source = 11 then 'douyin'             
                 else '其他' end as "平台",
            IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], 'Cross-border', 'Domestic')  AS "跨境"
            select 
            transform(alias_all_bid,{brand_in}, [1,2,3,4,5,6,7,8,9,10],999) rank,
            case when alias_all_bid  in {brand_in} then alias_all_bid
                else 0 end as alias_all_bid
            ,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), 'Others')"品牌"
            ,toStartOfMonth(pkey) AS Gmonth,
                       sum(sales)/100 AS columns1,
                       sum(num) AS columns2, 
                       sum(sales)/100/sum(num) as columns3,
                       sum (num* toFloat64OrZero(`spSKU件数`)) columns4
            from {tbl}
            {where}
            group by alias_all_bid,rank,Gmonth
        '''.format(tbl=tbl,brand_in=brand_in,where=players_where)

    ret = get_data(db,sql)
    df = pd.DataFrame(ret)
    df.loc[len(df)] = {'rank':'1','alias_all_bid':0,"品牌":'Others','Gmonth':'2024-12-01','columns1':0,'columns2':0,'columns3':0,'columns4':0}
    return df

def get_Format_report(db, tbl,brand_where, players_where):
    brand_in = get_top_brand(db, tbl, brand_where)
    sql = '''
            with
            case when source = 1 and shop_type < 20 and shop_type != 9 then 'tb'
                 when source = 1 and shop_type > 20 or source = 1 and shop_type = 9 then 'tmall'
                 when source = 2 then 'jd'
                 when source = 5 then 'kaola'        
                 when source = 6 then 'suning'        
                 when source = 9 then 'jiuxian'
                 when source = 11 then 'douyin'             
                 else '其他' end as "平台",
            IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], 'Cross-border', 'Domestic')  AS "跨境"
            select 
            transform(alias_all_bid,{brand_in}, [1,2,3,4,5,6,7,8,9,10],999) rank,
            case when alias_all_bid  in {brand_in} then alias_all_bid
                else 0 end as alias_all_bid
            ,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), 'Others')"品牌"
            ,toStartOfMonth(pkey) AS Gmonth,
                       sum(sales)/100 AS columns1,
                       sum(num) AS columns2, 
                       sum(sales)/100/sum(num) as columns3,
                       sum (num* toFloat64OrZero(`spSKU件数`)) columns4
            from {tbl}
            {where}
            group by alias_all_bid,rank,Gmonth
        '''.format(tbl=tbl,brand_in=brand_in,where=players_where)

    ret = get_data(db,sql)
    df = pd.DataFrame(ret)
    df.loc[len(df)] = {'rank':'1','alias_all_bid':0,"品牌":'Others','Gmonth':'2024-12-01','columns1':0,'columns2':0,'columns3':0,'columns4':0}
    return df

def get_data(db,sql,as_dict=True):

    data = db.query_all(sql,as_dict=as_dict)

    return data

def get_pivot_df(df,index, columns, values, aggfunc, fill_value):

    # 创建透视表
    pivot_df = df.pivot_table(
        index=index,
        columns=columns,
        values=values,
        aggfunc=aggfunc,
        fill_value=fill_value
    )

    pivot_df.reset_index(inplace=True)
    # print(pivot_df.shape)
    pivot_df.dropna(how='all', inplace=True)
    return pivot_df

def write_xlsx(data,row_s,col_s,work_book,sheet_name):
    sheet = work_book[sheet_name]
    max_row,max_col = data.shape
    data = np.array(data).tolist()
    data = [d[1:] for d in data]
    for r in range(max_row):
        for c in range(max_col-1):
            sheet.cell(row_s+r,col_s+c).value = data[r][c]
    return work_book,max_row+row_s

def run_Players_report(Players_dict,tbl,work_book):
    sheet_name = 'Key Players Performance'  # 报告要保存在哪一个sheet
    db = connect_clickhouse('chsop')

    row_s, cols = 7, 1
    for source in Players_dict:
        Player_dict = Players_dict[source]
        for players, where in Player_dict.items():
            df = get_Players_report(db, tbl, where['brand_where'], where['players_where'])
            index = ['rank', '品牌']
            columns = 'Gmonth'
            values = ['columns1', 'columns2', 'columns3', 'columns4']
            aggfunc = {
                'columns1': 'sum',
                'columns2': 'sum',
                'columns3': 'sum',
                'columns4': 'sum'
            }
            pivot_df = get_pivot_df(df, index=index, columns=columns, values=values, aggfunc=aggfunc,
                                    fill_value=0).head(-1)
            work_book, row_s = write_xlsx(pivot_df, row_s, 1, work_book, sheet_name)
            row_s += 1
        row_s += 1

def run_Format_report(Players_dict,tbl,work_book):
    sheet_name = 'Key Players Performance'  # 报告要保存在哪一个sheet
    db = connect_clickhouse('chsop')

    row_s, cols = 7, 1
    for source in Players_dict:
        Player_dict = Players_dict[source]
        for players, where in Player_dict.items():
            df = get_Players_report(db, tbl, where['brand_where'], where['players_where'])
            index = ['rank', '品牌']
            columns = 'Gmonth'
            values = ['columns1', 'columns2', 'columns3', 'columns4']
            aggfunc = {
                'columns1': 'sum',
                'columns2': 'sum',
                'columns3': 'sum',
                'columns4': 'sum'
            }
            pivot_df = get_pivot_df(df, index=index, columns=columns, values=values, aggfunc=aggfunc,
                                    fill_value=0).head(-1)
            work_book, row_s = write_xlsx(pivot_df, row_s, 1, work_book, sheet_name)
            row_s += 1
        row_s += 1

def get_Players_dict(brand_where,players_where):
    Players_dict = {
        'tb':{
            'Taobao': {
                'brand_where': brand_where + ''' 
                    and "平台"='tb'
                         ''',
                'players_where':players_where + ''' 
                and "平台"='tb'
                     '''
            },
            'Taobao&Domestic': {
                'brand_where': brand_where + '''
                    and "平台"='tb'
                    and "跨境"='Domestic'
                             ''',
                'players_where': players_where + '''
                    and "平台"='tb'
                    and "跨境"='Domestic'
                         '''
            },
            'Taobao&Cross-border': {
                'brand_where': brand_where + '''
                        and "平台"='tb'
                        and "跨境"='Cross-border'
                                 ''',
                'players_where': players_where + '''
                        and "平台"='tb'
                        and "跨境"='Cross-border'
                             '''
            }
        },
        # 'tmall':{
        #     'Tmall': {
        #         'brand_where': brand_where + '''
        #         and "平台"='tmall'
        #              ''',
        #         'players_where': players_where + '''
        #     and "平台"='tmall'
        #          '''
        #     },
        #     'Tmall&Domestic': {
        #         'brand_where': brand_where + '''
        #         and "平台"='tmall'
        #         and "跨境"='Domestic'
        #                  ''',
        #         'players_where': players_where + '''
        #         and "平台"='tmall'
        #         and "跨境"='Domestic'
        #              '''
        #     },
        #     'Tmall&Cross-border': {
        #         'brand_where': brand_where + '''
        #             and "平台"='tmall'
        #             and "跨境"='Cross-border'
        #                      ''',
        #         'players_where': players_where + '''
        #             and "平台"='tmall'
        #             and "跨境"='Cross-border'
        #                  '''
        #     }
        # },
        # 'JD':{
        #     'JD': {
        #         'brand_where': brand_where + '''
        #         and "平台"='jd'
        #              ''',
        #         'players_where': players_where + '''
        #     and "平台"='jd'
        #          '''
        #     },
        #     'JD&Domestic': {
        #         'brand_where': brand_where + '''
        #         and "平台"='jd'
        #         and "跨境"='Domestic'
        #                  ''',
        #         'players_where': players_where + '''
        #         and "平台"='jd'
        #         and "跨境"='Domestic'
        #              '''
        #     },
        #     'JD&Cross-border': {
        #         'brand_where': brand_where + '''
        #             and "平台"='jd'
        #             and "跨境"='Cross-border'
        #                      ''',
        #         'players_where': players_where + '''
        #             and "平台"='jd'
        #             and "跨境"='Cross-border'
        #                  '''
        #     }
        # },
        # 'Douyin':{
        #     'Douyin': {
        #         'brand_where': brand_where + '''
        #         and "平台"='douyin'
        #              ''',
        #         'players_where': players_where + '''
        #     and "平台"='douyin'
        #          '''
        #     },
        #     'Douyin&Domestic': {
        #         'brand_where': brand_where + '''
        #         and "平台"='douyin'
        #         and "跨境"='Domestic'
        #                  ''',
        #         'players_where': players_where + '''
        #         and "平台"='douyin'
        #         and "跨境"='Domestic'
        #              '''
        #     },
        #     'Douyin&Cross-border': {
        #         'brand_where': brand_where + '''
        #             and "平台"='douyin'
        #             and "跨境"='Cross-border'
        #                      ''',
        #         'players_where': players_where + '''
        #             and "平台"='douyin'
        #             and "跨境"='Cross-border'
        #                  '''
        #     }
        # }
    }
    return Players_dict
def main():
    #######################################  取报告可变的全局参数，可以任意改变  #############################################
    template = 'Eye Health Supplement in China EC Market - sample.xlsx'  # 报告魔板工作簿
    work_book = load_workbook(r'..\report\92312\\' + template)
    output = '91130 output_test.xlsx' #报告最终输出的工作簿名称
    tbl = 'sop_e.entity_prod_91130_E'  #从哪一张e表取报告
    Players_brand_where = '''
            where pkey>='2024-01-01' and pkey<'2024-12-01'
            and alias_all_bid not in (0,26,4023)
            and "sp子品类"='植物精华' 
            and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
        '''
    Players_players_where = '''
            where pkey>='2022-01-01' and pkey<'2025-01-01'
            and "sp子品类"='植物精华' 
            and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
        '''

    Format_brand_where = '''
                where pkey>='2024-01-01' and pkey<'2024-12-01'
                and alias_all_bid not in (0,26,4023)
                and "sp子品类"='植物精华' 
                and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
            '''
    Format_players_where = '''
                where pkey>='2022-01-01' and pkey<'2025-01-01'
                and "sp子品类"='植物精华' 
                and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
            '''
    Players_dict = get_Players_dict(Players_brand_where,Players_players_where)

    run_Players_report(Players_dict,tbl,work_book)
    work_book.save(r'..\report\92312\\' + output)

if __name__ == '__main__':
    main()
