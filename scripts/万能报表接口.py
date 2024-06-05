# -*- coding: utf-8 -*-
import requests
import pandas as pd
from bs4 import BeautifulSoup
def set_headers(url, data):
    eid = data['eid']
    headers = {
    'Accept': 'text/html, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Connection': 'keep-alive',
    'Content-Length': '284',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'user_login_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsImtpZCI6InMyMDE4MDkwNCJ9.eyJ1c2VyX2lkIjoiMTY4IiwiZXhwIjoxNzE3NDM3NTgyfQ.OsSyU-hiCglhyvGNDnWR_PN8VJYVdu6EKB_lthYMZUU',
    'Host': 'chsql.ecdataway.com',
    'Origin': 'https://chsql.ecdataway.com',
    'Referer': 'https://chsql.ecdataway.com/web/report',
    'Sec-Ch-Ua': '"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?1',
    'Sec-Ch-Ua-Platform': '"Android"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36 Edg/125.0.0.0',
    'X-Requested-With': 'XMLHttpRequest'
}
    return headers

###万能报表查询接口
def get_url(url, data):
    response = requests.post(url, headers=set_headers(url, data), data=data)
    # 检查响应状态码
    if response.status_code == 200:
        print(response.json())
        # 获取响应的内容
        html_content = response.content
        # 创建BeautifulSoup对象
        soup = BeautifulSoup(html_content, 'html.parser')

        # 找到表格
        table = soup.find('table')

        # 提取所有行
        rows = table.find_all('tr')

        # 提取表头和数据
        data = []
        for row in rows:
            cols = row.find_all(['th', 'td'])
            cols = [ele.text.strip() for ele in cols]
            data.append(cols)

        # 创建DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        # 显示DataFrame
        return df

    else:
        print("请求失败，状态码：", response.status_code)
        return


if __name__ == "__main__":
    url = 'https://chsql.ecdataway.com/web/ajax-report-html'
    data = {
        'site': 'ali',
        'eid': 0,
        'eid_tb': '',
        'group_by_row': 'gPlatform',
        'group_by_col': '',
        'group_by_sum': '',
        'group_by_col_val': 'num_total',
        'group_by_alternative': '',
        'w_start_date': '2024-05-01',
        'w_end_date_exclude': '2024-05-31',
        'platformsIn': 'all',
        'platformsInText': '阿里全部'
    }
    df = get_url(url, data)
    print(df)
