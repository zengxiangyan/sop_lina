# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
from datetime import timedelta
import openpyxl
from bs4 import BeautifulSoup
def set_headers(url, data):
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Content-Length': '877',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'QS_USER_KEY=a1c0e1308dbc4923a452e68cf51f85d5; cna=0hQ9HXDKO3MCAXxPSplSNOKX; _npk_id.9.ee34=7d69babf0a9fd846.1713867331.; _npk_id.1.ee34=4a005efc3dd4b389.1715846762.; Hm_lvt_20c2ab8b1854ba60ed8fb40e638c46f8=1715846762; _npk_id.3.ee34=11fc1ea0ab884d7e.1717148744.; PHPSESSID=eab88d5k29or23qc60n92mo5r6; session_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsImtpZCI6InMyMDE4MDkwNCJ9.eyJlYWI4OGQ1azI5b3IyM3FjNjBuOTJtbzVyNiI6IiJ9.WHudlE-0LfXdtpviDkSMbF0BdnC4bCL7bgAzohlf8hM; _npk_id.4.ee34=7d6052dc21d89793.1717479116.; _npk_ses.4.ee34=1; Hm_lvt_affd25576ddfe22ae74dcb75a04ede69=1717479116; _npk_ref.1.ee34=%5B%22%22%2C%22%22%2C1717479234%2C%22https%3A%2F%2Fsop.ecdataway.com%2F%22%5D; _npk_ses.1.ee34=1; Hm_lpvt_20c2ab8b1854ba60ed8fb40e638c46f8=1717479234; _clck=1wqest8%7C2%7Cfmc%7C0%7C1376; _clsk=1uw26k7%7C1717479234860%7C1%7C1%7Cu.clarity.ms%2Fcollect; QBTPHPSESSID=d93ef7020c58f2321d5dd770a525cc84; language=en_us; Hm_lpvt_affd25576ddfe22ae74dcb75a04ede69=1717479261',
        'Host': 'art.nint.com',
        'Origin': 'https://art.nint.com',
        'Referer': 'https://art.nint.com/solutions/?site=universal2',
        'Sec-Ch-Ua': '"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
        'X-Requested-With': 'XMLHttpRequest'
    }
    return headers

def rule_format(type=None,red = False,):
    if red and type == 'header':
        rule = {
            'bold': True,
            'border': False,
            'text_wrap': False,
            'valign': 'vcenter',
            'align': 'center',
            'font_name': '等线',
            'font_color': 'red',
            'font_size': 11,
            'bg_color': '#D7E4BC'
        }
    elif type == 'header':
        rule = {
            'bold': True,
            'border': False,
            'text_wrap': False,
            'valign': 'vcenter',
            'align': 'center',
            'font_name': '等线',
            'font_color': 'black',
            'font_size': 11,
            'bg_color': '#D7E4BC'
        }
    elif red and type == 'cell':
        rule = {
            'font_name': '等线',
            'font_color': 'red',
            'font_size': 11,
            'valign': 'vcenter',
            'align': 'left'
        }
    else:
        rule = {
            'font_name': '等线',
            'font_size': 11,
            'valign': 'vcenter',
            'align': 'left'
        }
    return rule

def get_time_dif(start_time):
    """
    获取已经使用的时间
    :param start_time:
    :return:
    """
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))
def pandas_save(brand_info,cat_info,df1, df2,filenames):
    brand_info = pd.DataFrame(brand_info)
    cat_info = pd.DataFrame(cat_info)
    df1 = pd.DataFrame(df1)
    df2 = pd.DataFrame(df2)
    dataframes = [brand_info,cat_info,df1, df2]

    # 创建一个映射，将工作表名称映射到对应的DataFrame
    df_map = dict(zip(filenames, dataframes))
    # 创建一个ExcelWriter对象
    with pd.ExcelWriter(f'./全域数据下载{int(time.time())}.xlsx',engine='xlsxwriter',options={'strings_to_urls': False,'constant_memory':False}) as writer:
        # 将DataFrame写入不同的工作表
        for df, filename in zip(dataframes, filenames):
            start_time = time.time()
            df.to_excel(writer, sheet_name=filename, float_format='%.2f', index=False)
            print(f"pandas 保存 {filename} 耗时：", get_time_dif(start_time))
        # 获取workbook对象
        workbook = writer.book

        start_time = time.time()
        # 应用格式到每个工作表
        for filename in filenames:

            # 获取当前工作表的worksheet对象
            worksheet = writer.sheets[filename]

            # 获取当前工作表对应的DataFrame
            df = df_map[filename]

            worksheet.set_column('A:I', None, workbook.add_format(rule_format(type='cell')))
            # 应用标题行的格式
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, workbook.add_format(rule_format(type='header')))
    print(f"pandas 设置格式 耗时：", get_time_dif(start_time))
###万能报表查询接口
def get_url(data_data):
    url = 'https://art.nint.com/solutions/solutions/ajax-slide'
    data = {
        'type': 'P057_cid',
        'time_params': f"time_string,{data_data['s_date']},{data_data['e_date']}",
        'cids': ','.join(data_data['cid_list']),
        'cid': data_data['cid_list'][0],
        'platforms': ','.join(data_data['platform_list']),
        'bids': ','.join(data_data['bid_list']),
        'site': 'universal2',
        'cid_ver': '0'
    }
    print(data)
    response = requests.post(url, headers=set_headers(url, data), data=data)
    # 检查响应状态码
    if response.status_code == 200:
        # 创建DataFrame
        try:
            brand_info = pd.DataFrame(response.json().get('brand_info'))
        except:
            print(response.json())
        cat_info = pd.DataFrame(response.json().get('cat_info'))
        df1 = pd.DataFrame(response.json().get('data1'))
        df2 = pd.DataFrame(response.json().get('data2'))

        return brand_info,cat_info,df1,df2

    else:
        print("请求失败，状态码：", response.status_code)
        return

def run():
    brand_info, cat_info, df1, df2 = get_url(data_data)
    pandas_save(brand_info, cat_info, df1, df2, ['brand_info', 'cat_info', 'df1', 'df2'])
    return 'SUCCESSFULLY'

if __name__ == "__main__":
    data_data_list = [{
        's_date': '2024-01-01',
        'e_date': '2024-02-01',
        'cid_list': ['16'],
        'platform_list': ['all','ali_tmall','ali_tb','jd','dy'],
        'bid_list': ['11425','57832'],
    },
        {
            's_date': '2024-02-01',
            'e_date': '2024-03-01',
            'cid_list': ['16'],
            'platform_list': ['all', 'ali_tmall', 'ali_tb', 'jd', 'dy'],
            'bid_list': ['11425', '57832'],
        }
    ]
    for data_data in data_data_list:
        run()
