import logging
import time
import os

class Logger:
    def __init__(self,logName,filename):
        self.logger = logging.getLogger(logName)
        self.logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(filename,mode='w')  #If mode is not specified, 'a' is used.
        handler.setLevel(logging.NOTSET)
        formatter = logging.Formatter('%(asctime)s %(name)s %(thread)d %(threadName)s %(levelname)s:%(message)s',datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
   
    def debug(self,msg):
        if self.logger is not None:
            self.logger.debug(msg)
            
    def info(self,msg):
        if self.logger is not None:
            self.logger.info(msg)

    def warning(self,msg):
        if self.logger is not None:
            self.logger.warning(msg)

    def error(self,msg):
        if self.logger is not None:
            self.logger.error(msg)

    def critical(self,msg):
        if self.logger is not None:
            self.logger.critical(msg)

normal = Logger("NORMAL",os.path.abspath(os.curdir)+'\\log\\debug\\'+time.strftime('normal_%Y_%m_%d_%H_%M_%S',time.localtime(time.time())) + ".log")
attention = Logger("ATTENTION",os.path.abspath(os.curdir)+'\\log\\debug\\'+time.strftime('attention_%Y_%m_%d_%H_%M_%S',time.localtime(time.time())) + ".log")

if __name__ == "__main__":
    normal.debug("hello")
    attention.critical("2world")
