from wrapper import *
import numpy as np
def HighPrice(start,end):
    db.connDB()
    stockTuple = db.getStockAll()
    highPrice = []
    for (id,code) in stockTuple:
        price_today = db.getPriceCertainDay(id,end)
        print ("code:%d"%code)
        if price_today == None:
            continue
        else:
            stock_price = db.getPriceByCode(code,start)
            mark = 0
            for i in stock_price:
                if i[3] > price_today[5]:
                    mark = 1
                    break
            if mark == 0:
                highPrice.append(code)
    db.closeDB()
    return highPrice

def strategyOne():
    db.connDB()
    stockTuple = db.getStockAll()
    for (id,code) in stockTuple:
        stock_price = db.getPrice(id,'2016-8-20')
        if len(stock_price[:10]) == 10:
            close = []
            log_1.logger.debug("###############   Start to handle data : %s  ##############"%code)
            print ("###############   Start to handle data : %s  ##############"%code)
            for i in stock_price[:5]:
                close.append(i[4])
            close = np.array(close)
            var = np.std(close)
            log_1.logger.debug("###############   Result : %f  ##############"%var)
    db.closeDB()

if __name__ == "__main__":
    db = DBoperator(table['code'],table['price'])
    strategyOne()
