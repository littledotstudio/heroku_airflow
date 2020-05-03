from bs4 import BeautifulSoup
import urllib.request
from fake_useragent import UserAgent
import csv
import time
import pandas as pd
import requests
import re
import numpy as np
import os
import psycopg2
from datetime import date

today = date.today()
DATABASE_URL = os.environ['DATABASE_URL']

shops = ['LittleDotStudio', 'LetterandLeather', 'AGIOTAGE', 'GiftsAndTravel', 'SoGoodSoWood', 'MyPersonalMemories', 'IndexUrban', 'OxandPine', 'geneofstyle', 'shelterislandpaper']
df_shop = pd.DataFrame()
def striphtml(data):
    p = re.compile(r'<.*?>')
    s = re.compile(r'Sales')
    temp = p.sub('', data)
    temp2 = s.sub('',temp)
    return temp2

today = date.today()
def scrape_sales(shop):
    url = "http://api.scraperapi.com/?api_key=73822ad1ccc1340538f30989d8ac042d&url=https://www.etsy.com/shop/" + shop
    ua = UserAgent()
    page_search = requests.get(url, {"User-Agent": ua.random})
    soup = BeautifulSoup(page_search.content,"html.parser")
    div = soup.findAll("div", {"class": "pt-xs-3"})[0]
    sales = striphtml(str(div))
    return(sales)

for shop in shops:
    sales = int(scrape_sales(shop))
    df = pd.DataFrame({'shop': [shop], 
                       'dt': [today], 
                       'sales': [sales]})
    df_shop = df_shop.append(df)

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cur = conn.cursor()

sales_table_insert = ("""
    INSERT INTO sales (shop, dt, sales)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

for i, row in df_shop.iterrows():
    cur.execute(sales_table_insert, row)
    conn.commit()