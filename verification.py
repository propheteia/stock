import tushare as ts
from log import *
from wrapper import *
from update import *
from build import *
import time
import datetime
from multiprocessing.dummy import Pool as ThreadPool

verifySucceed = []
verifyFailed = []
verifyFailedDueToNetwork = []
logVerify_= Logger("Verify",os.path.abspath(os.curdir)+'\\log\\'+time.strftime('Verify_%Y%m%d_%H%M%S',time.localtime(time.time())) + ".log")

def verify(stock,p):
    start = time.clock()
    global verifyFailedDueToNetwork
    stockToBeVerified = [getCodeAndPrice(adjustCode(s)) for s in stock]
    count = 0
    while True:
        verifyFailedDueToNetwork = []
        verify_multi(stockToBeVerified,p)
        if len(verifyFailedDueToNetwork) >=1 and count < 2:
            attention.info("######## Start to Verified Failed Stock Once More")
            attention.info(verifyFailedDueToNetwork)
            stockToBeVerified = [getCodeAndPrice(adjustCode(s)) for s in verifyFailedDueToNetwork]
            count += 1
        else:
            end = time.clock()
            duration = (end -start)/60
            verifyLog(duration) 
            return verifyFailed

def renew(stock):
    for s in stock:
        db.deletePriceByCode(s)
        s2 = db.getStockByOneCode(int(s))
        #insertSinglePriceToDB(s2)
        download_multi(s2,4)
    verify(stock,4)

def verify_multi(codeAndPrice,p):
    pool = ThreadPool(processes=p)
    pool.map(verifyData,codeAndPrice)
    pool.close()
    pool.join()

def verifyLog(duration):
    logVerify_.info("######## Verify Result (Total %d minutes)########"%duration)
    logVerify_.info("Stock Passed:%d"%len(verifySucceed))
    logVerify_.info("Stock Not Passed:%d"%len(verifyFailed))
    if len(verifyFailed):
        logVerify_.info(verifyFailed)
    logVerify_.info("Stock Not Passed Duo To Network:%d"%len(verifyFailedDueToNetwork))
    if len(verifyFailedDueToNetwork):
        logVerify_.info(verifyFailedDueToNetwork)

def getCodeAndPrice(code): # code is string
    code = code
    dataFromSql = db.getPriceByCode(code,'2016-01-01')
    return (code,dataFromSql)

# verify one stock by code, price is also provided for multi-thread 
def verifyData(codeAndPrice): # codeAndPrice = (code,price) (string,tuple)
    global verifySucceed
    global verifyFailed
    global verifyFailedDueToNetwork
    code = codeAndPrice[0]
    dataFromSql = codeAndPrice[1]

    normal.debug("######## Start To Verify Stock:%s ########"%code)
    normal.debug("Stock:%s Getting Stock Data From Tushare"%(code))
    try:
        start = time.clock()
        dataFromTS = ts.get_h_data(str(code), start='2016-01-01',end=time.strftime("%Y-%m-%d",time.localtime()))
        end = time.clock()
        normal.debug("Data:")
        normal.debug(dataFromTS)
    except Exception as e:
        end = time.clock()
        normal.warning("Stock:%s Network Problem To Get Data From Tushare"%(code))
        normal.warning(e)
        dataFromTS = False

    #dataFromTS = get_hfq_data(codeAndPrice[0],'2015-01-01',time.strftime("%Y-%m-%d",time.localtime()))
    if dataFromTS is False:
        normal.debug("######## Stock:%s Finished: Network Problem ########"%(code))
        attention.debug("######## Stock:%s Finished: Network Problem ########"%(code))
        verifyFailedDueToNetwork.append(code)
        print ("Stock:%s ... Network Problem ... Time:%d"%(code,(end-start)))
        return
    elif dataFromTS is None:
        normal.debug("######## Stock:%s Finished: No Data ########"%(code))
        print ("Stock:%s ... No Data ... Time:%d"%(code,(end-start)))
        return
    else:
        print ("Stock:%s ... Get Data ... Time:%d"%(code,(end-start)))
        dataFromTS = dataFromTS.reset_index()
        sub_set = dataFromTS[['date','open','high','close','low','volume','amount']]
        dataFromTS_toTuple = tuple([tuple(convertToDatetime(x)) for x in sub_set.values])
    if dataFromTS_toTuple == dataFromSql:
        verifySucceed.append(code)
        normal.debug("######## Stock:%s Finished: Pass ########"%(code))
    else:
        normal.debug("Stock:%s Not Pass.Start to Compare... "%(code))
        if compareTwoTuples(dataFromSql,dataFromTS_toTuple,code):
            verifySucceed.append(code)
            normal.debug("######## Stock:%s Finished: Pass ########"%(code))
        else:
            verifyFailed.append(code)
            normal.debug("######## Stock:%s Finished: Not Pass ########"%(code))
            attention.debug("######## Stock:%s Finished: Not Pass ########"%(code))
    return 

def compareTwoTuples(sql,ts,code):
    if len(sql) == len(ts):
        normal.debug("Stock:%s Same Length"%code)
        for i in range(len(sql)):
            if sql[i] == ts[i]:
                continue
            else:
                normal.debug("SQL:")
                normal.debug(sql[i])
                normal.debug("TS:")
                normal.debug(ts[i])
                for j in range(1,len(sql[i])):
                    if abs(sql[i][j] - ts[i][j]) <= 0.02:
                        normal.debug("Ignore the small diffenets")
                    else:
                        attention.debug("SQL:")
                        attention.debug(sql[i])
                        attention.debug("TS:")
                        attention.debug(ts[i])
                        attention.warning("######## Stock:%s Not Pass -- Different values ########"%(code))
                        return False
    else:
        normal.debug("Stock:%s Different Length"%code)
        attention.debug("Stock:%s Not Pass -- Different Length"%code)
        return False
    return True

#    print (type(dataFromTS_toTuple))
    
def convertToDatetime(x):
    x[0] = x[0].to_datetime().date()#strftime("%Y-%m-%d")
    return x

def ttest():
    z = ts.get_h_data('300001','2016-10-20','2016-10-25')

if __name__ == "__main__":
    db = DBoperator(table['code'],table['price'])
    stockList = [i[0] for i in db.getStockCodeAll()]
    failList = verify(stockList,10)
    renew(failList)
    db.closeDB()
