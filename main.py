import os
import sys
import pandas as pd
# from DataAccessPackage.readDayFile import readDayFile
from DataAccessPackage.sadataBigquery import bigQueryData, readCsvFile
# from DataAccessPackage.utilityBigquery import bigQueryData, bigQueryIn
from CommonPackage.common import common, loggerCommon
from CommonPackage.logger import *


if __name__ == '__main__':
    try:
        print('Running...')
        
        # bigQueryData.updateBrandName()
        # bigQueryData.updateKeywordDate()
        
        # bigQueryData.deleteData()
        
        # 일별 데이터로 파싱
        readCsvFile.readXlsx()
        
        print('Stopped.')
    
    except Exception as e:
        print(e)


