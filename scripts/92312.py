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
            ,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), 'others')"品牌"
            ,toStartOfMonth(pkey) AS Gmonth,
                       sum(sales)/100 AS sales_value,
                       sum(num) AS sales_volume, 
                       sum(sales)/100/sum(num) as Price_perUnit,
                       sum (num* toFloat64OrZero(`spSKU件数`)) "销售件数"
            from {tbl}
            {where}
            group by alias_all_bid,rank,Gmonth
        '''.format(tbl=tbl,brand_in=brand_in,where=players_where)

    ret = get_data(db,sql)
    df = pd.DataFrame(ret)
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

def get_sku_list(db,tbl):
    sql = """
            select 
            "spSKU名" ||"spSKU容量" "対象製品" ,SUM(sales)`销售额`
            FROM {tbl}
            WHERE  pkey>='2023-01-01' and pkey<'2024-01-01'
                and alias_all_bid=218724 --限制黛珂
                and "source" in (1,2,11) --4个平台
                and "sp是否人工答题" in ('前后月覆盖','出题宝贝')              
                and "spSKU名" not in ('LIPOSOME  ADVANCED REPAIR SERUM','____其它','___待客户确认(同规格限定/替换)')
            GROUP BY "対象製品"
            ORDER BY `销售额` DESC;
            """.format(tbl=tbl)
    sku_list = db.query_all(sql,as_dict=False)

    return [sku[0] for sku in sku_list]

def get_sku_report(db,tbl,sku_list,row_s,col_s,work_book,model_sheet_name,sheet_name,group_date=False):
    for i,sku in enumerate(sku_list):
        where = """ WHERE  pkey>='2023-01-01' and pkey<'2024-01-01'
                and alias_all_bid=218724 --限制黛珂
                and "平台" in ('JD','Douyin','Tmall','Taobao') --4个平台
                and "sp是否人工答题" in ('前后月覆盖','出题宝贝')    
                and ("spSKU名" ||"spSKU容量") = '{sku}' """.format(sku=sku)
        price_data = get_item_info(db=db,where=where,group_date=group_date,tbl=tbl)
        sql = get_sku_info(where=where,group_date=group_date, tbl=tbl)
        data = get_data(db, sql, as_dict=False)
        rr = pd.DataFrame(data, columns=['対象メーカー','対象製品','容量','ECプラットフォーム','年月','店舗タイプ', '店舗名', '金額規模（元）','販売量規模（個数）', '取引個数単価（元/1本あたり）', 'sid'])
        rr = pd.merge(rr, price_data, on=['対象メーカー','対象製品','容量','ECプラットフォーム','年月','店舗タイプ', '店舗名','sid'], how='left', suffixes=('_total', '_item'))
        data = np.array(rr).tolist()
        rr = [[sku,'','','','','','','','','','','','',''],['202307','','','','','','','','','','','','',''],['対象メーカー','対象製品','容量','ECプラットフォーム','年月','店舗タイプ', '店舗名', '金額規模（元）','販売量規模（個数）', '取引個数単価（元/1本あたり）', 'sid','item_sales','item_volume','item_Price_perUnit']] + data + [['', '', '', '', '', '','','','','',''], ['', '', '', '', '', '','','','','','']]
        rr = pd.DataFrame(rr, columns=['対象メーカー','対象製品','容量','ECプラットフォーム','年月','店舗タイプ', '店舗名', '金額規模（元）','販売量規模（個数）', '取引個数単価（元/1本あたり）', 'sid','item_sales','item_volume','item_Price_perUnit'])
        work_book,row_s = write_xlsx(rr, row_s, col_s, work_book, model_sheet_name, sheet_name)
    return work_book

def style_map(rule):

    style_style = {
        'sku_title':(3,1),
        'date_title':(4,1),
        'col_title':(5,1),
        'total_row':(18,1),
        'normal':(6,1)
    }
    if rule in style_style.keys():
        return style_style[rule]
    return

def get_style(wb,model_sheet_name,sheet_name,from_cell,to_cell):
    # 加载现有的 Excel 文件

    # 选择工作表
    ws = wb[model_sheet_name]

    # 获取源单元格
    source_cell = ws.cell(from_cell[0],from_cell[1])

    # 创建新的样式对象
    new_font = Font(name=source_cell.font.name, size=source_cell.font.size,bold=source_cell.font.bold,italic=source_cell.font.italic,vertAlign=source_cell.font.vertAlign,underline=source_cell.font.underline,strike=source_cell.font.strike,color=source_cell.font.color)
    new_fill = PatternFill(fill_type=source_cell.fill.fill_type,start_color=source_cell.fill.start_color,end_color=source_cell.fill.end_color)
    new_border = Border(left=source_cell.border.left,right=source_cell.border.right, top=source_cell.border.top,bottom=source_cell.border.bottom,diagonal=source_cell.border.diagonal,diagonal_direction=source_cell.border.diagonal_direction,outline=source_cell.border.outline)
    new_alignment = Alignment(horizontal=source_cell.alignment.horizontal,vertical=source_cell.alignment.vertical,text_rotation=source_cell.alignment.text_rotation,wrap_text=source_cell.alignment.wrap_text,shrink_to_fit=source_cell.alignment.shrink_to_fit,indent=source_cell.alignment.indent)
    new_protection = Protection(locked=source_cell.protection.locked, hidden=source_cell.protection.hidden)

    # 应用新的样式对象到目标单元格
    ws = wb[sheet_name]
    target_cell = ws.cell(to_cell[0],to_cell[1])
    target_cell.font = new_font
    target_cell.fill = new_fill
    target_cell.border = new_border
    target_cell.alignment = new_alignment
    target_cell.number_format = source_cell.number_format
    target_cell.protection = new_protection
    return wb

def write_xlsx(data,row_s,col_s,work_book,model_sheet_name,sheet_name):

    max_row,max_col = data.shape
    data = np.array(data).tolist()
    for r in range(max_row):
        for c in range(max_col):
            if r == 0:
                work_book = get_style(wb=work_book,model_sheet_name=model_sheet_name,sheet_name=sheet_name,from_cell=style_map('sku_title'),to_cell=(row_s+r,col_s+c))
            elif r == 1:
                work_book = get_style(wb=work_book,model_sheet_name=model_sheet_name,sheet_name=sheet_name, from_cell=style_map('date_title'),to_cell=(row_s + r, col_s + c))
            elif r == 2:
                work_book = get_style(wb=work_book,model_sheet_name=model_sheet_name,sheet_name=sheet_name, from_cell=style_map('col_title'),to_cell=(row_s + r, col_s + c))
            elif r == max_row-3:
                work_book = get_style(wb=work_book,model_sheet_name=model_sheet_name,sheet_name=sheet_name, from_cell=style_map('total_row'),to_cell=(row_s + r, col_s + c))
            elif r < max_row-3:
                work_book = get_style(wb=work_book,model_sheet_name=model_sheet_name,sheet_name=sheet_name, from_cell=style_map('normal'),to_cell=(row_s + r, col_s + c))
            else:
                pass
            sheet = work_book[sheet_name]
            sheet.cell(row_s+r,col_s+c).value = data[r][c]
    return work_book,max_row+row_s


def main():
    #######################################  取报告可变的全局参数，可以任意改变  #############################################

    tbl = 'sop_e.entity_prod_91130_E'  #从哪一张e表取报告
    brand_where = '''
            where pkey>='2024-01-01' and pkey<'2024-12-01'
            and alias_all_bid not in (0,26,4023)
            and "sp子品类"='植物精华' 
            and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
        '''
    players_where = '''
            where pkey>='2022-01-01' and pkey<'2024-12-01'
            and "sp子品类"='植物精华' 
            and or ("sp种类"='叶黄素',"sp复合种类" in ('蓝莓叶黄素','越橘叶黄素胡萝卜素','叶黄素越橘','叶黄素越橘蓝莓','蓝莓黑加仑叶黄素','野樱莓叶黄素','蓝莓叶黄素β-胡萝卜','虾青素叶黄素'))
        '''

    Players_dict = {
        'Taobao': {
            'brand_where': brand_where + ''' 
                and "平台"='tb'
                     ''',
            'players_where':players_where + ''' 
            and "平台"='tb'
                 '''
        },
        # 'Taobao&Domestic': {
        #     'brand_where': brand_where + '''
        #         and "平台"='tb'
        #         and "跨境"='Domestic'
        #                  ''',
        #     'players_where': players_where + '''
        #         and "平台"='tb'
        #         and "跨境"='Domestic'
        #              '''
        # }

    }

    template = 'コーセー様_納品データ_231120.xlsx' #报告魔板工作簿
    output = '91130 output.xlsx' #报告最终输出的工作簿名称
    model_sheet_name = '模板' #报告模板在哪一个sheet
    sheet_name = '报告' #报告要保存在哪一个sheet

    db = connect_clickhouse('chsop')
    # work_book = load_workbook(r'..\report\91130\\' + template)
    for players,where in Players_dict.items():
        df = get_Players_report(db,tbl,where['brand_where'],where['players_where'])
        index = ['rank','品牌']
        columns = 'Gmonth'
        values = ['sales_value', 'sales_volume', 'Price_perUnit']
        aggfunc = {
            'sales_value': 'sum',
            'sales_volume': 'sum',
            'Price_perUnit': 'sum',
            # '销售件数':'销售件数'
        }
        pivot_df = get_pivot_df(df, index=index, columns=columns, values=values, aggfunc=aggfunc,
                                fill_value=0)
        if players == 'Taobao':
            data = pivot_df
        else:
            data = pd.concat([data, pivot_df], axis=0, ignore_index=True)
    data.to_excel(r'..\report\92312\\' + output,encoding='utf-8-sig')
if __name__ == '__main__':
    main()