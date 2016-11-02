import re
import urllib3
from log import *
from bs4 import BeautifulSoup

def parse_stock_list():
    url =  "http://quote.eastmoney.com/stocklist.html"
    http = urllib3.PoolManager()
    r = http.request("GET",url)
    html = r.data.decode('gbk')
    soup=BeautifulSoup(html)
    patt_view_60 = 'http://quote.eastmoney.com/sh60\d\d\d\d'
    patt_view_30 = 'http://quote.eastmoney.com/sz30\d\d\d\d'
    patt_view_00 = 'http://quote.eastmoney.com/sz00\d\d\d\d'
    patt_60 = re.compile(patt_view_60,re.MULTILINE)
    patt_30 = re.compile(patt_view_30,re.MULTILINE)
    patt_00 = re.compile(patt_view_00,re.MULTILINE)
    result = patt_60.findall(html)+patt_00.findall(html)+patt_30.findall(html)
    for  i in result:
        content = soup.find_all("a",href=i+".html")
        text = content[0].text
        normal.debug(text)
        m = re.match(r'''(\D+)(\()(\d{6})(\))''',text)
        text = m.group(1,3)
        yield text

test2=(("qilushihua",700003),("handangangtie",700004))
        
if __name__ == "__main__":
    for i in parse_stock_list():
        print (i)
#    a = parse_stock_list()
#    for i in a:
#        z = i[0].encode('utf-8').decode('utf-8')
#        print (z)
    #parse_stock_list()
    #m = re.match(r'(\w+) (\w+)', 'hello world!')
    #print (m.group(3))

