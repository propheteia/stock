#-*- coding:utf-8 -*-
import pymysql
import MySQLdb
from log import *
import time
import datetime

# username,host,password,database
mysql_login = ["jimmy", "localhost","jimshu1989","tu_data"]
table = {'code':'aaa','price':'bbb'}

class DBoperator(object):
    def __init__(self,code_table,price_table):
        self.connection= None
        self.stock_table = code_table
        self.price_table = price_table
        self.connDB()
        
    def connDB(self):
        print ("############### Connect database ######################")
        try:
            self.connection = pymysql.connect(
                    host = mysql_login[1],
                    user = mysql_login[0],
                    db = mysql_login[3],
                    charset="utf8",
                    password = mysql_login[2])
            normal.debug("############### connected to database ######################")
        except:
            attention.warning("Failed to connecte to database")
        
    def closeDB(self):
        print ("############### Close database ######################")
        if self.connection != None:
            self.connection.close()
            normal.debug("############### close database ######################")
        else:
            attention.warning("Failed to close database")

    def createPriceTable(self):
        print ("Building Price table...")
        with self.connection.cursor() as cursor:
            sql = """create table %s(
                id int(11) NOT NULL auto_increment,
                stock_id int(11) default NULL,
                date date default null,
                open double default null,
                close double default null,
                high double default null,
                low double default null,
                volume double default null,
                amount double default null,
                Primary key (id),
                UNIQUE key date_stock (date,stock_id), 
                key stock_id (stock_id),
                constraint %s_ibfk_1 foreign key(stock_id) references %s (id)
                )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""%(self.price_table,self.price_table,self.code_table)
            cursor.execute(sql)

    def createCodeTable(self):
        print ("Building Stock table...")
        with self.connection.cursor() as cursor:
            sql = """create table %s(
                id int(11) NOT NULL auto_increment,
                name char(30),
                code int(11),
                UNIQUE(code),
                Primary key (id))ENGINE=InnoDB DEFAULT CHARSET=utf8;"""%self.code_table
            cursor.execute(sql)

    def addCodeToDB(self,stock):
        print ("########## Start to add %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))
        normal.debug("########## Start to add %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))
        with self.connection.cursor() as cursor:
            sql = """insert into %s (name,code) values("%s",%d);
            """%(self.code_table,stock[0],int(stock[1]))
            try:
                b = cursor.execute(sql)
                self.connection.commit()
                normal.debug("########## Success to add %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))
                return b
            except Exception as e:
                normal.debug("########## Add %s : %s  to %s  Failed###########"%(stock[0],stock[1],self.code_table))
                normal.warning(e)
                attention.debug("########## Add %s : %s  to %s  Failed###########"%(stock[0],stock[1],self.code_table))
    def updateCodeToDB(self,stock):
        print ("########## Start to update %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))
        normal.debug("########## Start to update %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))
        with self.connection.cursor() as cursor:
            sql = """insert ignore into %s (name,code) values("%s",%d);
            """%(self.code_table,stock[0],int(stock[1]))
            try:
                b = cursor.execute(sql)
                c = self.connection.commit()
                if b==1:
                    attention.debug("########## Success to add new code %s : %s  to %s ###########"%(stock[0],stock[1],self.code_table))

            except Exception as e:
                normal.debug("########## Update %s : %s  to %s  Failed###########"%(stock[0],stock[1],self.code_table))
                normal.warning(e)
                attention.debug("########## Update %s : %s  to %s  Failed###########"%(stock[0],stock[1],self.code_table))

    def getStockID(self,code):
        with self.connection.cursor() as cursor:
            sql = "select id from " + self.stock_table + " where code = %s "%(code)
            cursor.execute(sql)
            stock_id = cursor.fetchone() 
        return stock_id      # ·µ»Øtuple¶ÔÏó
    
    def getStockByCode(self,code):
        with self.connection.cursor() as cursor:
            sql = "select id,code from " + self.stock_table + " where code in (" + ','.join(code) + ") order by id;";
            cursor.execute(sql)
            stock= cursor.fetchall()
        return stock

    def getStockByOneCode(self,code):
        with self.connection.cursor() as cursor:
            sql = "select id,code from " + self.stock_table + " where code = %d;"%code
            cursor.execute(sql)
            stock= cursor.fetchall()
        return stock

    def getStockAll(self):
        with self.connection.cursor() as cursor:
            #sql = "select id,code from " + self.stock_table + " where id >700" + " order by id;";
            sql = "select id,code from " + self.stock_table + " order by id;";
            cursor.execute(sql)
            stock= cursor.fetchall()
        return stock

    def getStockCodeAll(self):
        with self.connection.cursor() as cursor:
            #sql = "select code from " + self.stock_table + " where id >910 and id<920" + " order by id;";
            sql = "select code from " + self.stock_table + " order by id;";
            cursor.execute(sql)
            stock= cursor.fetchall()
        return stock

    def getStockCode(self,stock_id):
        with self.connection.cursor() as cursor:
            sql = "select code from " + self.stock_table + " where id = %s "%(stock_id)
            cursor.execute(sql)
            code = cursor.fetchone()
        return code

    def getPriceByCode(self,code,start,end="curdate()"):
        stock_id = self.getStockID(code)
        with self.connection.cursor() as cursor:
            sql = "select date,open,high,close,low,volume,amount from " + self.price_table + " where stock_id = %d"%stock_id  + \
                    " and date between '%s'" %start+ \
                    " and %s"%end +\
                    " order by date(date) DESC;" 
            cursor.execute(sql)
            price = cursor.fetchall()
            return price

    def getPriceCertainDay(self,stock_id,date):
        with self.connection.cursor() as cursor:
            sql = "select * from " + self.price_table + " where stock_id = %d"%stock_id  + \
                    " and date = '%s'"%date
            cursor.execute(sql)
            price = cursor.fetchone()
            return price

    def getMaxDay(self,stock_id):
        with self.connection.cursor() as cursor:
            sql = "select max(date) from " + self.price_table + " where stock_id = %d;"%stock_id
            cursor.execute(sql)
            maxday = cursor.fetchone()
            cursor.close()
        return maxday

    def deletePriceByCode(self,code):
        with self.connection.cursor() as cursor:
            sql = "delete from " + self.price_table + " where stock_id = %d;"%(self.getStockID(code))
            cursor.execute(sql)
            self.connection.commit()
            cursor.close()
        
if __name__ == "__main__":
    db = DBoperator(table['code'],table['price'])
