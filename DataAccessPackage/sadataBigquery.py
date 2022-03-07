import os
import sys
import datetime
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from CommonPackage.logger import *
from CommonPackage.common import common, loggerCommon
import numpy as np

class readCsvFile:
    def readCoupang():
        # tsv 파일 - 한달치 데이터 -> 일별데이터
        fileName = 'A00001914_pa_daily_keyword_20210901_20210930'
        df1 = pd.read_csv("D:/프로젝트/partice/readCsv/A00001914_pa_daily_keyword_20211001_20211031.tsv", sep='\t', low_memory=False, index_col=False)
        df2 = df1.reset_index()

        dates = df1['날짜'].drop_duplicates()
        for i in dates:
            df2 = df1[df1['날짜'] == i]
            df2.to_csv('D:/test/' + 'A00001914_pa_daily_keyword_' + str(i) + '_' + str(i) + '.csv', encoding='utf-8-sig', columns=df2.columns, index=False)

        # for i in range(len(df1)):
        #     cond1 = (df2['index'] == i)
        #     df3 = df2.loc[cond1].rename(columns={'index':''}).set_index('')
        #     df1.to_csv('D:/test/' + 'A00001914_pa_daily_keyword_' + str(i) + '_' + str(i) + '.csv', encoding='utf-8-sig', columns=df1.columns, index=False)

class bigQueryDac:
    def __init__(self) -> None:
        self.bigQuery_client = bigquery.Client()
        
    def updateQuery(self, query, tableId):
        result = None
        try:
            query_job = self.bigQuery_client.query(query)
            
            query_job.result()
            # print(f"{tableId} - modified {query_job.num_dml_affected_rows} rows.")
            print(query)
            result = query_job.num_dml_affected_rows
            
        except Exception as e:
            print(e)

        return result

    def selectQuery(self, query):
        try:
            results = self.bigQuery_client.query(query)
            return results.to_dataframe()

        except Exception as e:
            print(e)

            return None


class bigQueryData:
    def __init__(self) -> None:
        pass
    
    def updateBrandName() :
        # Brand_Name, Product_Name Update
        result = None
        try:
            dac = bigQueryDac()
            
            tableName = 'ap-bq-mart.AP_Bigdata_Dashboard_US.SAData_Total_'
            tableDate = datetime.datetime(2022,2,20)
            
            # 일별 테이블
            while tableDate < datetime.datetime(2022,3,1) :
                tableId = tableName + tableDate.strftime('%Y%m%d')
                
                query = f'''
                        UPDATE `{tableId}` s
                        SET s.Brand_Name = (
                            select m.Brand_Name
                            from `ap-bq-mart.AP_Bigdata_Dashboard_US.BrandNameMapping` m
                            where m.Product_Id = s.Product_Id and m.Site = s.Site
                        ), s.Product_Name = (
                            select m.Product_Name
                            from `ap-bq-mart.AP_Bigdata_Dashboard_US.BrandNameMapping` m
                            where m.Product_Id = s.Product_Id and m.Site = s.Site
                        )
                        where s.Brand_Name is null
                '''
                dac.updateQuery(query, tableId)
                tableDate = tableDate + datetime.timedelta(days=1)
                
            result = dac.selectQuery(query)

        except Exception as e:
            print(e)

        return result
    
    # 키워드 타입 분류 업데이트
    def updateKeywordDate():
        result = None
        try:
            dac = bigQueryDac()
            tableName = 'ap-bq-mart.AP_Bigdata_Dashboard_US.SAData_Total_'
            tableDate = datetime.datetime(2022,1,1)
            
            while tableDate < datetime.datetime(2022,2,1) :
                tableId = tableName + tableDate.strftime('%Y%m%d')
                query = f'''
                        update `{tableId}` s
                        set K_Type_D1 = d1, K_Type_D2 = d2, K_Type_D3 = d3
                        from (
                            select KeyWord, max(Depth_1) d1, max(Depth_2) d2, max(Depth_3) d3
                            from `ap-bq-mart.AP_Bigdata_Dashboard_US.KeywordMapping` k
                            group by KeyWord
                        ) k
                        where s.K_Type_D1 is null and s.Keyword = k.Keyword
                    '''
                result_query = dac.updateQuery(query, tableId)    
                print(result_query)
                
                tableDate = tableDate + datetime.timedelta(days=1)
            
        except Exception as e:
            print(e)
    
    # 쿼리문 다시하기
    def updateKeywordType() :
        try:
            data = pd.read_excel("D:/키워드 분류.xlsx", index_col=None)
            
            dac = bigQueryDac()
            tableName = 'ap-bq-mart.AP_Bigdata_Dashboard_US.SAData_Total_'
            
            for row in data.itertuples():
                query = f'''
                        SELECT Date FROM `ap-bq-mart.AP_Bigdata_Dashboard_US.SAData_Total_*`
                        Where Keyword = '{row[1]}' and K_Type_D1 is null
                        group by Date
                        order by Date
                '''
                resultDate = dac.selectQuery(query)
                
                if len(resultDate) < 1:
                    print(row[1])
                    continue
                
                # 쿼리문 수정해야 함
                for dd in resultDate.itertuples():
                    tableDate = dd[1].strftime("%Y%m%d")
                    tableId = "ap-bq-mart.AP_Bigdata_Dashboard_US.SAData_Total_" + tableDate
                    
                    if pd.isna(row[3]) :
                        query = f'''
                                update `{tableId}` s
                                set K_Type_D1 = '{row[2]}'
                                where Keyword = '{row[1]}' 
                            '''
                    elif pd.isna(row[4]) :
                        query = f'''
                                update `{tableId}` s
                                set K_Type_D1 = '{row[2]}', K_Type_D2 = '{row[3]}'
                                where Keyword = '{row[1]}' 
                            '''
                    else:
                        query = f'''
                                update `{tableId}` s
                                set K_Type_D1 = '{row[2]}', K_Type_D2 = '{row[3]}', K_Type_D3 = '{row[4]}'
                                where Keyword = '{row[1]}' 
                            '''
                    result_query = dac.updateQuery(query, tableId)
                    
            result = dac.selectQuery(query)
            
        except Exception as e:
            print(e)
    
    def  deleteCompaign():
        # 불필요한 쿠팡 데이터 삭제 요청
        try:
            data = pd.read_excel("D:/BE2_2022/쿠팡삭제.xlsx", index_col=None, sheet_name="삭제 대상")
            
            dac = bigQueryDac()
            tableName = "ap-bq-mart.AP_Bigdata_Dashboard.SAData_Total_"
            
            for row in data.itertuples():
                query = f'''
                        SELECT Date FROM `ap-bq-mart.AP_Bigdata_Dashboard.SAData_Total_*`
                        Where Campaign = '{row[1]}'
                        group by Date
                        order by Date       
                '''
                resultDate = dac.selectQuery(query)
                
                if len(resultDate) < 1:  # 삭제할 데이터 없을 때
                    continue
                
                for dd in resultDate.itertuples():
                    tableDate = dd[1].strftime("%Y%m%d")
                    tableId = "ap-bq-mart.AP_Bigdata_Dashboard.SAData_Total_" + tableDate
                    query = f'''
                            DELETE `{tableId}` 
                            WHERE Site = '쿠팡' AND Campaign = '{row[1]}'
                        '''
                    result_query = dac.updateQuery(query, tableId)
                print(row[1] + " end")  # 끝
            
        except Exception as e:
            print(e)

    def insertKeyword():
        try:
            data = pd.read_excel("D:/보정확인_11번가 Product Info DB_F_골든플래닛.xlsx", index_col=None)
            
            dac = bigQueryDac()
            table_name = "ap-bq-mart.AP_Bigdata_Dashboard.BrandNameMapping"
            
            for row in data.itertuples():
                query = f'''
                        INSERT INTO `ap-bq-mart.AP_Bigdata_Dashboard.BrandNameMapping` (Product_Id, Brand_Name, Site, Product_Name)
                        VALUES ('{row[1]}','{row[3]}','11번가','{row[2]}')
                '''
                result_query = dac.updateQuery(query, table_name)
            
            # for row in df1.itertuples():
            #     query = f'''
            #             INSERT INTO {table_name} (Product_Id, Brand_Name, Site, Product_Name)
            #             VALUES ('{str(row[1])}','{row[2]}','{row[3]}','{row[4]}')
            #             '''
            #     result_query = dac.updateQuery(query, table_name)
              
            # for i in df:
            #     data = df[i]
            #     df2 = data[['상품번호', '상품명', '브랜드']]
            #     for row in df2.itertuples():
            #         query = f'''
            #             INSERT INTO {table_name} (Product_Id, Brand_Name, Site, Product_Name)
            #             VALUES ('{row[1]}','{row[3]}','위메프','{row[2]}')
            #         '''
            #         result_query = dac.updateQuery(query, table_name)
            #     print('next')
        
        except Exception as e:
            print(e)