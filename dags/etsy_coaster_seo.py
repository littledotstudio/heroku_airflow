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
    etsy_url = "https://www.etsy.com/search/?q=" + query
    ua = UserAgent()

    etsy_page_search = requests.get(etsy_url, {"User-Agent": ua.random})
    soup_search = BeautifulSoup(etsy_page_search.content,"html", features="html.parser")
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
        temp_shop = soup_product.find_all("meta")[24]
        temp_shop2 = re.search(r'meta content="(.*?)" property=', str(temp_shop)).group(1)
        shop.append(temp_shop2)

        keywords_list = soup_product.find_all("a", {"class":"btn btn-secondary"})

        temp_tags = []
        for i in range(0, len(keywords_list)):
            temp_tags2 = re.search(r'blank">(.*?)</a>', str(keywords_list[i])).group(1)
            temp_tags.append(temp_tags2)

        temp_tags = [x for x in temp_tags if "&" not in x ]
        #temp_tags = " ".join(word for word in temp_tags).lower()
        tags.append(temp_tags)
        #time.sleep(1)

    shop = [x.strip("https://www.etsy.com/shop/") for x in shop]
    df = pd.DataFrame({'list_id': list_id_records, 
                       'title': title_records, 
                       'tags': tags,
                       'descriptions': descriptions,
                       'shop': shop})
    return(df)


coasters_search = ['Coasters', 'coaster']

df = pd.DataFrame()

for i in range(0, len(coasters_search)):
    print("Getting data for tag: ", coasters_search[i])
    df_temp = get_query_df(coasters_search[i])
    df_temp['date'] = today
    df_temp['rank'] = np.arange(len(df_temp))
    df_temp['tag'] = coasters_search[i]
    df = df.append(df_temp)


conn = psycopg2.connect(DATABASE_URL, sslmode='require')
conn.set_session(autocommit=True)


def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
# Connecting to the database

# Inserting each row
# Inserting each row
for i in df.index:
    query = """
    INSERT into seo(id, title, tags, description, shop, dt, rank, tag) values(%s,%s,%s);
    """ % (df['id'], df['title'], df['tags'],df['description'], df['shop'], df['dt'], df['rank'], df['tag'])
    single_insert(conn, query)
# Close the connection
conn.close()
