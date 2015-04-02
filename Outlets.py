import requests
from bs4 import BeautifulSoup
import pyodbc
from translate import Translator
from datetime import datetime
import re

source_code = requests.get(
    "http://www.premiumoutlets.com/outlets/sales.asp?id=71")
plain_text = source_code.text
soup = BeautifulSoup(plain_text)

for item_name in soup.findAll('div', {'class': 'StoreEvents'}):
    web_data = item_name

web_data_str = unicode.join(u'\n',map(unicode,web_data))
coupons = web_data_str.split(
    "<div style=\"border-top: 1px solid #E2E2E2; margin:8px 0 6px 0;padding:0;position:relative;\"></div>")

translator= Translator(to_lang="zh")

cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=xwmilyml1x.database.windows.net;\
    DATABASE=Coupon;UID=SGTSUser;PWD=Abc1234$')
cursor = cnxn.cursor()

for coupon in coupons:
    coupon = coupon.replace("<h4 class=\"cap mb-10\">","")\
             .replace("</h4>","").replace("<br>","").replace("</br>","")
    coupon_split = re.split('\r', coupon)
    coupon_split = filter(None, coupon_split)
    store_name = coupon_split[0]
    date_string = coupon_split[1].replace(",","").replace("\n","")
    coupon_split.pop(0)
    coupon_split.pop(0)
    description = ""
    description_cn = ""
    for sentence in coupon_split:
        try:
            description += sentence
        except Exception:
            pass
    for sentence_cn in coupon_split:
        try:
            description_cn += translator.translate(sentence_cn)
        except Exception:
            pass
    
    if len(date_string.split(" ")) == 5:
        start_date_string = date_string.split(" ")[0] + ' ' + \
                            date_string.split(" ")[1] + ' ' + date_string.split(" ")[-1]
        end_date_string = date_string.split(" ")[0] + ' ' + \
                          date_string.split(" ")[3] + ' ' + date_string.split(" ")[-1]
        start_date = datetime.strptime(start_date_string, '%b %d %Y')
        end_date = datetime.strptime(end_date_string, '%b %d %Y')
        cursor.execute('INSERT INTO [dbo].[Stores] \
                        ([store_name],[start_date],[end_date],[description],[description_cn]) \
                        values (?,?,?,?,?)', \
                       store_name,start_date,end_date,description,description_cn)
        cnxn.commit()
        print(store_name)
        print(start_date)
        print(end_date)
        print(description)
        print(description_cn)

    elif len(date_string.split(" ")) == 6:
        start_date_string = date_string.split(" ")[0] + ' ' + \
                            date_string.split(" ")[1] + ' ' + date_string.split(" ")[-1]
        end_date_string = date_string.split(" ")[3] + ' ' + \
                          date_string.split(" ")[4] + ' ' + date_string.split(" ")[-1]
        start_date = datetime.strptime(start_date_string, '%b %d %Y')
        end_date = datetime.strptime(end_date_string, '%b %d %Y')
        cursor.execute('INSERT INTO [dbo].[Stores] \
                        ([store_name],[start_date],[end_date],[description],[description_cn]) \
                        values (?,?,?,?,?)', \
                       store_name,start_date,end_date,description,description_cn)
        cnxn.commit()
        print(store_name)
        print(start_date)
        print(end_date)
        print(description)
        print(description_cn)

    elif len(date_string.split(" ")) == 7:
        start_date_string = date_string.split(" ")[0] + ' ' + \
                            date_string.split(" ")[1] + ' ' + date_string.split(" ")[-1]
        end_date_string = date_string.split(" ")[4] + ' ' + \
                          date_string.split(" ")[5] + ' ' + date_string.split(" ")[-1]
        start_date = datetime.strptime(start_date_string, '%b %d %Y')
        end_date = datetime.strptime(end_date_string, '%b %d %Y')
        cursor.execute('INSERT INTO [dbo].[Stores] \
                        ([store_name],[start_date],[end_date],[description],[description_cn]) \
                        values (?,?,?,?,?)', \
                       store_name,start_date,end_date,description,description_cn)
        cnxn.commit()
        print(store_name)
        print(start_date)
        print(end_date)
        print(description)
        print(description_cn)
