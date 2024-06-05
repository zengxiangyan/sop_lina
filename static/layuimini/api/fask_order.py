# -*- coding: utf-8 -*-
import datetime

import pandas as pd
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import os
import subprocess
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_driver():
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = Chrome(service=Service(), options=chrome_options)
    # 注意：把chromedriver文件放到了当前文件夹里面，可以这样调用
    # driver = Chrome('./chromedriver.exe', options=chrome_options)
    return driver


def env_xpath(xpath):
    xpath_dic = {"立即购买": "/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[6]/div[1]/button/span",
                 "提交订单": '/html/body/div[1]/div[3]/div/div[1]/div[1]/div/div[9]/div/div/a'}
    return xpath_dic[xpath]


def get_ele_text(driver, xpath):
    try:
        ele_text = driver.find_element(By.XPATH, env_xpath(xpath)).text
        return ele_text
    except:
        try:
            ele_text = driver.find_element(By.XPATH, env_xpath("立即购买")).text
            return ele_text
        except:
            return ''

def get_product_info(driver):
    num = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[1]/div/span').text
    price = driver.find_element(By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[2]/div/div/div/div/div/span[3]').text

    print(num,price)

def now_buy(url, not_buy=True, retry_max=5):
    driver = get_driver()
    wait = WebDriverWait(driver, 0.5)
    retry = 0
    tt = (datetime.datetime.strptime('2023-12-07 14:59:58', '%Y-%m-%d %H:%M:%S')-datetime.datetime.now()).total_seconds()
    while tt>=1:
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        time.sleep(1)
        tt-=1
    driver.get(url)
    wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[6]/div[1]/button/span')))
    while not_buy and tt>0 and retry<=retry_max:
        ele_text = get_ele_text(driver, '立即购买')
        if ele_text == '立即购买':
            driver.find_element(By.XPATH, env_xpath("立即购买")).click()
        ele_text = get_ele_text(driver, '提交订单')
        if ele_text == '提交订单':
            not_buy = False
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"loading... " + ele_text)
            return driver
        else:
            retry += 1
            driver.refresh()
            wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[3]/div/div[2]/div[2]/div[1]/div/div[2]/div[6]/div[1]/button/span')))
            tt = (datetime.datetime.strptime('2023-12-07 15:00:00', '%Y-%m-%d %H:%M:%S')-datetime.datetime.now()).total_seconds()
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"loading... " + ele_text)
            pass

    return driver
if __name__ == "__main__":
    # os.system("chrome_start.cmd")
    # chrome.exe --remote-debugging-port=9222 --user-data-dir="D:\selenium-chrome"

    # 设置命令行命令
    # cmd = ["start","","C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"]
    # # 使用 subprocess.Popen 执行命令
    # # 使用 subprocess.Popen 执行命令，不等待其完成
    # process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)

    # url = 'https://detail.tmall.com/item.htm?id=702916527846&skuid=5124078447550'
    url = 'https://detail.tmall.com/item.htm?_u=t2dmg8j26111&id=702916527846&spm=a1z0k.7385961.1997985097.d4918993.1b6337deSGgkAd&sku_properties=5919063:6536025'
    url = 'https://buy.tmall.com/order/confirm_order.htm?x-itemid=702916527846&spm=pc_detail.27183998/evo365560b447259evo401275b518001.455144&x-uid=2438820932'#提交订单页面，不确定不同期的是否参数会有变化
    # url = 'https://cart.taobao.com/cart.htm?spm=a220l.1.0.0.34da7f33Q3B4q3&from=btop'#购物车页面
    now_buy(url)