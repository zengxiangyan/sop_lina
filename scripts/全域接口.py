# -*- coding: utf-8 -*-
import requests
import pandas as pd
from bs4 import BeautifulSoup
def set_headers(url, data):
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '877',
        'Content-Type': 'multipart/form-data; boundary=----WebKitFormBoundarybYjXIdSfotFlmoAu',
        'Cookie': 'QS_USER_KEY=de3107f1b599bb3b6205b3a4243f16b5; _npk_id.4.ee34=6be9690c306661e9.1715147006.; _npk_id.3.ee34=33c5fe5ed4f54dfb.1715596604.; _npk_id.1.ee34=35e1200ce5b430f6.1715681097.; _npk_id.9.ee34=41cc58ce747e37c1.1716447131.; cna=qfraHg5SRRQCAd5GawNWupmF; Hm_lvt_20c2ab8b1854ba60ed8fb40e638c46f8=1715681097,1716276712,1716452635,1716877341; _npk_ref.1.ee34=%5B%22%22%2C%22%22%2C1716877341%2C%22https%3A%2F%2Fres.ecdataway.com%2F%22%5D; _clck=6feycx%7C2%7Cfm5%7C0%7C1595; Hm_lvt_affd25576ddfe22ae74dcb75a04ede69=1716871703,1717145988,1717382731,1717391859; PHPSESSID=2fck657o31p5pnn57ef69chect; session_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsImtpZCI6InMyMDE4MDkwNCJ9.eyIyZmNrNjU3bzMxcDVwbm41N2VmNjljaGVjdCI6IiJ9.gYLd17Qo2Xik5xbOpSSpfKWNaL2GAHwpEVNNG6IQ--8; QBTPHPSESSID=01890e9b7acc9c3ee9b01995176a96fb; language=en_us; _npk_ses.4.ee34=1; Hm_lpvt_affd25576ddfe22ae74dcb75a04ede69=1717470156; kadis_user_login_token2=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsImtpZCI6InMyMDE4MDkwNCJ9.eyJ1c2VyX2lkIjozMjYsImV4cCI6MTcxNzA1MTkzMH0.wRxzIX3YC30Mi_Q8-MWWcQHpYWUqSCqvw-n5hf9EJTc; _npk_ses.9.ee34=1',
        'Host': 'art.nint.com',
        'Origin': 'https://art.nint.com',
        'Referer': 'https://art.nint.com/solutions/?site=universal2',
        'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    return headers

###万能报表查询接口
def get_url(url, data):
    response = requests.post(url, headers=set_headers(url, data), data=data)
    # 检查响应状态码
    if response.status_code == 200:
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
    url = 'https://art.nint.com/solutions/solutions/ajax-slide'
    data = {
        "type": "P057_cid",
        "time_params": {
            "time_string": "2024-2-1",
            "end_time": "2024-03-01"
        },
        "cids": [16],
        "cid": 16,
        "platforms": ["all", "ali_tmall", "ali_tb", "jd", "dy"],
        "bids": [11425, 57832],
        "site": "universal2",
        "cid_ver": 0
    }
    df = get_url(url, data)
    print(df)
