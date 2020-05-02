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


def get_query_df(query):
    query = query
    etsy_url = "http://api.scraperapi.com/?api_key=73822ad1ccc1340538f30989d8ac042d&url=https://www.etsy.com/search/?q=" + query
    ua = UserAgent()
    etsy_page_search = requests.get(etsy_url, {"User-Agent": ua.random})
    soup_search = BeautifulSoup(etsy_page_search.content,"html.parser")
    #This is the listing id list
    listing_id = soup_search.find_all("a")
    #This holds the listing url
    list_id_records = []
    title_records = []
    tags = []
    descriptions = []
    shop = []
    #this gather listing url by listing id and adding to website address
    for listing in listing_id:
        list_id = (listing.get("data-listing-id"))
        title_id = (listing.get("title"))
        if list_id != None and title_id != None:
            url_product = "http://www.etsy.com/listing/" + str(list_id) +"/"
            list_id_records.append(url_product)
            title_records.append(title_id)
    for i in range(0, len(list_id_records)):
        etsy_page_product = requests.get(list_id_records[i], {"User-Agent": ua.random})
        soup_product = BeautifulSoup(etsy_page_product.content,"html.parser")
        # Descriptions
        temp_description = soup_product.find_all("meta")[15]
        temp_description2 = re.search(r'meta content="(.*?)" name=', str(temp_description)).group(1)
        descriptions.append(temp_description2)
        # Shop Owner
        temp_shop = soup_product.find_all("meta")[26]
        temp_shop2 = re.search(r'meta content="(.*?)" property=', str(temp_shop)).group(1)
        shop.append(temp_shop2)
        keyword_list = soup_product.find_all("h2")[3]
        temp_tags = re.search(r'name">(.*?)</span>', str(keyword_list))
        temp_tags2 = temp_tags.group(1)
        #time.sleep(1)
    shop = [x.strip("https://www.etsy.com/shop/") for x in shop]
    df = pd.DataFrame({'list_id': list_id_records, 
                       'title': title_records, 
                       'tags': temp_tags2,
                       'descriptions': descriptions,
                       'shop': shop})
    return(df)


luggage_search = ["luggage tag", "luggage tags", "travel tag"]
df = pd.DataFrame()

for i in range(0, len(luggage_search)):
    print("Getting data for tag: ", luggage_search[i])
    df_temp = get_query_df(luggage_search[i])
    df_temp['dt'] = today
    df_temp['rank'] = np.arange(len(df_temp))
    df_temp['tag'] = luggage_search[i]
    df = df.append(df_temp)

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
cur = conn.cursor()

seo_table_insert = ("""
    INSERT INTO seo (id, title, tags, description, shop, dt, rank, tag)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

for i, row in df.iterrows():
    cur.execute(seo_table_insert, row)
    conn.commit()
