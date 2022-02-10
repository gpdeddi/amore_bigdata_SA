import pandas as pd
import logging
import linecache
import os
import sys
import datetime

from CommonPackage.logger import *
from CommonPackage.common import *

logger_filePath = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/.logs/bigquery_extract.log'  
logger = customLogger('cs_logger', logger_filePath).getinstance()


class common:
    def __init__(self) -> None:
        pass


    def getNowDateTime(format):
        result = ''
        try:
            now = datetime.datetime.now()
            result = now.strftime(format)
        
        except Exception as ex:
            exc_type, exc_obj, tb = sys.exc_info()
            exc_str = loggerCommon.getException(tb, exc_obj)
            logger.error(exc_str)
        
        return result


    def getStringDateAddDay(format):
        result = ''
        try:
            now = datetime.datetime.now()
            td = datetime.timedelta(days=-1)
            date = now + td
            result = date.strftime(format)
        
        except Exception as ex:
            exc_type, exc_obj, tb = sys.exc_info()
            exc_str = loggerCommon.getException(tb, exc_obj)
            logger.error(exc_str)
        
        return result    


    def saveFile(df, table_name):
        result = None
        try:
            path = f'{os.path.abspath(os.getcwd())}\\data\\'
            date = common.getNowDateTime('%Y%m%d%H%M%S')
            file_name = f'{path}BigQuery_{table_name}_{date}.csv'

            df.to_csv(file_name, index=False)
            result = file_name
        
        except Exception as ex:
            exc_type, exc_obj, tb = sys.exc_info()
            exc_str = loggerCommon.getException(tb, exc_obj)
            logger.error(exc_str)
        
        return result

    
    def convertString(list, sepStr = ','):
        result = ''

        try:    
            result = sepStr.join(list)

        except Exception as ex:
            exc_type, exc_obj, tb = sys.exc_info()
            exc_str = loggerCommon.getException(tb, exc_obj)
            logger.error(exc_str)

        return result
    



    

class loggerCommon:
    def __init__(self) -> None:
        pass

    
    def getException(tb, exc_obj):
        result = ''
    
        try:
            lineno = tb.tb_lineno

            f = tb.tb_frame
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            result = f'EXCEPTION IN ({filename}, LINE {lineno} "{line.strip()}"): {exc_obj}'

        except Exception as ex :
            result = ''

        return result
        

if __name__ == '__main__':
    try:
        print(datetime.now())
        print(common.getDate(datetime.now()))
        print(datetime.now())

    except Exception as e:
        logging.error(RuntimeError(f'[ERROR][__main__] : {e}'))