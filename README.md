# BigQuery Data Extractor
빅쿼리 데이터 csv 파일로 저장하는 프로그램   


## - 실행 방법
### # command
1. 날짜 범위 데이터 가져오기
```
.venv/Script/activate   // 가상 환경 실행
python main.py INPUT1 INPUT2 [INPUT3] [INPUT4] [INPUT5] [INPUT6]
```

2. 쿼리 실행
```
.venv/Script/activate   // 가상 환경 실행
python main.py INPUT1 INPUT2
```

<br>

### # directory
```
BigQuery_Extract
ㄴ  /.lib
|   ㄴ  requirements.txt    // pip install package
|
ㄴ  /.logs                  // 자동 생성                      
ㄴ  /.venv                      
ㄴ  /CommonPackage
|   ㄴ  common.py
|   ㄴ  logger.py
|
ㄴ  /data                   // csv 파일 저장 위치 ***(없을 경우 강제 생성해줘야함)***
ㄴ  /DataAccessPackage
|   ㄴ utilityBigQuery
|
ㄴ  main.py                 // launch file 
ㄴ  README.md
```

<br>

### # Input Data

1. 날짜 범위 데이터 가져오기

| 이름 | 설명 | 예시 | 타입 | 필수 여부 | 기본 값 |
|:---:|---|---|:---:|:---:|---|
INPUT1 | 추출 할 빅쿼리 테이블 | tb_SocialData | String | Y | - |
INPUT2 | 시작 날짜 (yyyymmdd) | 20211211 | String | Y | - |
INPUT3 | 조건 | colume like 'test%' | String | N | None |
INPUT4 | 종료 날짜 (yyyymmdd) | 20211212 | String | N | 오늘 날짜 - 1일 |
INPUT5 | 컬럼 | Idx, CompanyName, CompanyCode | String | N | * (전체 컬럼) |
INPUT6 | 빅쿼리 데이터 세트 | tousflux-cloud.tousflux_datafeeding_bq | String | N | tousflux-cloud.tousflux_datafeeding_bq |


2. 쿼리 실행

| 이름 | 설명 | 예시 | 타입 | 필수 여부 | 기본 값 |
|:---:|---|---|:---:|:---:|---|
INPUT1 | 쿼리 실행 식별자 | Q | String | Y | 쿼리로 프로그램을 실행할 경우 Q(대문자) 입력 |
INPUT2 | 작동 할 쿼리 | SELECT * FROM tousflux-cloud.tousflux_datafeed_bq.tb_SocialData_20211227 WHERE Round in (2, 3) | String | Y | - |

