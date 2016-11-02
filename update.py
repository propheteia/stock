import numpy as np
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
import time

successedToUpdate = []
failedDueToNetwork = []
failedDueToSql = []
log_= Logger("Update",os.path.abspath(os.curdir)+'\\log\\'+time.strftime('Update_%Y%m%d_%H%M%S',time.localtime(time.time())) + ".log")

def update(stock,p):
    # Updata stock code list
    #for stock in parse_stock_list():
        #db.updateCodeToDB(stock)
    start = time.clock()
    global failedDueToNetwork 
    stockToBeUpdated = [list(i) + list(db.getMaxDay(i[0])) for i in db.getStockByCode(stock)]
    count = 0
    while True:
        failedDueToNetwork = []
        update_multi(stockToBeUpdated,p)
        if len(failedDueToNetwork) >=1 and count < 2:
            attention.info("######## Start to Update Failed Stock Once More")
            attention.info(failedDueToNetwork)
            stockToBeUpdated= [list(i) + list(db.getMaxDay(i[0])) for i in db.getStockByCode(failedDueToNetwork)]
            count += 1
        else:
            end = time.clock()
            duration = (end -start)/60
            updateLog(duration)
            return

def updateLog(duration):
    log_.info("######## Update Result (Total %d minutes)########"%duration)
    log_.info("Updated Stock: %d"%len(successedToUpdate))
    log_.info("Failed Stock Due To Network:%d "%len(failedDueToNetwork))
    if len(failedDueToNetwork):
        log_.info(failedDueToNetwork)
    log_.info("Failed Stock Due To SQL:%d"%len(failedDueToSql))
    if len(failedDueToSql):
        log_.info(failedDueToSql)

def update_multi(stock_all,p):
    pool = ThreadPool(processes=p)
    pool.map(updatePriceToDB,stock_all)
    pool.close()
    pool.join()

def updatePriceToDB(stock):# stock is a tuple object (id,code)
    global successedToUpdate
    global failedDueToNetwork 
    global failedDueToSql
    id = stock[0]
    code = adjustCode(stock[1])
    print ("######## Stock:%s Start to Update ..."%(code),end=" ")
    start = time.clock()
    normal.debug("######## Stock:%s Start to Update ########"%(code))
    try:
        start_day = stock[2]
    except:
        start_day = None
    end_day = time.strftime("%Y-%m-%d",time.localtime())
    normal.debug("Last:%s -- Now:%s"%(start_day,end_day))
    if start_day is None: 
        normal.debug("Stock:%s: No Data Exist In The Database"%(code))
        start_day='2014-01-01'
        iAmExist = False
    else:
        start_day += datetime.timedelta(days=1)
        start_day = start_day.strftime("%Y-%m-%d")
        iAmExist = True

    normal.debug("Stock:%s Getting Stock Data From Tushare"%(code))
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
        failedDueToNetwork.append(code)
        end = time.clock()
        print ("Network Problem  Time:%d"%(end-start))
        return
    elif df is None:
        if iAmExist is False:
            #attention.warning("Stock:%s Is Not Exist ########"%(code))
            normal.warning("Stock:%s Is Not Exist ########"%(code))
            print ("")
        else:
            successedToUpdate.append(code)
            normal.debug("Stock:%s Is UpToDate"%(code))
            end = time.clock()
            print ("Uptodate  Time:%d"%(end-start))
        normal.debug("######## Stock:%s Finished: No Data ########"%(code))
        return
    else:
        if iAmExist is False:
            attention.warning("Stock:%s Is A New Stock ########"%(code))
            normal.warning("Stock:%s Is A New Stock ########"%(code))
    normal.debug("Add Foreigh Key(stock_id)")
    df['stock_id'] = id
    normal.debug("Inserting Data To MySQL")
    engine = 'mysql://jimmy:jimshu1989@127.0.0.1/tu_data?charset=utf8'
    try:
        df.to_sql(db.price_table,engine,if_exists='append')
        result = True
    except Exception as e:
        normal.warning("Inserting to MySQL Failed")
        normal.warning ("ERROR TYPE: %s"%e.args)
        result = False
    #result = tu.insert_to_sql(df,db.price_table)
    if result:
        end = time.clock()
        normal.debug("########  Stock:%s Finished:Succeed Time:%ds########"%(code,end-start))
        successedToUpdate.append(code)
        print ("Succeed Time:%ds"%(end-start))
    else:
        normal.debug("########  Stock:%s Finished:MySQL problem ########"%(code))
        attention.debug("########  Stock:%s Finished:MySQL problem ########"%(code))
        print ("MySQL Problem")
    return

def adjustCode(code):
    return str(code).zfill(6)

def testNetwork(code):
    print ("##### %s #####"%code)
    s = ts.get_h_data(code,start='2016-10-21',end='2016-10-24')
    print (s)

if __name__ == "__main__":
    db = DBoperator(table['code'],table['price'])
    stockCodeList = [str(i[0]) for i in db.getStockCodeAll()]
    update(stockCodeList,12) 
    db.closeDB()


