import numpy as np
from update import adjustCode
from sqlalchemy import create_engine
from wrapper import *
import tushare as ts
import pandas as pd
from log import *
import time
import sys
from crawler import *
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

table = {'code':'aaa','price':'bbb'}

#################### Building Database ########################
#   1      2       3       4       5       6       7       8    
#  stock  date    open    close   high    low   volume   amount

def buildStockDB(stock_gen):
    db.createCodeTable()
    for stock in stock_gen:
        db.addCodeToDB(stock)
    db.createPriceTable()
    download_multi(db.getStockAll(),10)

def download_multi(stock_all,p):
    pool = ThreadPool(processes=p)
    pool.map(insertSinglePriceToDB,stock_all)
    pool.close()
    pool.join()
    
def insertSinglePriceToDB(stock): # stock:(id,code)
    id = stock[0]
    code = adjustCode(stock[1])
    print ("######## Stock:%s Start to Update ########"%(code))
    normal.debug("######## Stock:%s Start to Update ########"%(code))
    start_day='2014-01-01'
    end_day = time.strftime("%Y-%m-%d",time.localtime())
    try:
        df = ts.get_h_data(code,start=start_day,end=end_day)
        normal.debug("Data:")
        normal.debug(df)
    except Exception as e:
        normal.warning("Stock:%s Network Problem To Get Data From Tushare"%(code))
        normal.warning("ERROR TYPE: %s"%e.args)
        df= False

    if df is False: 
        normal.debug("######## Stock:%s Finished: Network Problem ########"%(code))
        attention.warning("######## Stock:%s Finished: NetworkProblem ########"%(code))
        return
    elif df is None:
        normal.debug("######## Stock:%s Finished: No Data ########"%(code))
        attention.warning("######## Stock:%s Finished: No Data ########"%(code))
        return
    normal.debug("Add Foreigh Key(stock_id)")
    df['stock_id'] = id

    try:
        df.to_sql(db.price_table,engine,if_exists='append')
        result = True
    except Exception as e:
        normal.warning("Inserting to MySQL Failed")
        normal.warning ("ERROR TYPE: %s"%e.args)
        result = False

    if result:
        normal.debug("######## Stock:%s Finished:Succeed ########"%(code))
        print ("######## Stock:%s Finished: Succeed ########"%(code))
    else:
        normal.debug("######## Stock:%s Finished:MySQL problem ########"%(code))
        attention.debug("######## Stock:%s Finished:MySQL problem ########"%(code))
    return

