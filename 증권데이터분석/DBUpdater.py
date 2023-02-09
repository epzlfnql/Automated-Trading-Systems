import pymysql
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup

class DBUpdater:
    def __init__(self):
        '''생성자: MariaDB 연결 및 종목코드 딕셔너리 생성'''
        self.conn = pymysql.connect(host = 'localhost', user='root',
        password = 'ghdtnsduf1', db = 'INVESTAR', charset='utf8')

        with self.conn.cursor() as curs:
            sql = '''CREATE TABLE if NOT EXISTS company_info(
	CODE VARCHAR(20),
	company VARCHAR(40), 
	last_update DATE,
	 PRIMARY KEY(CODE)
)'''
            curs.execute(sql)
            
            sql = '''CREATE TABLE IF NOT EXISTS daily_price(
	CODE VARCHAR(20),
	DATE DATE,
	OPEN BIGINT(20),
	high BIGINT(20),
	low BIGINT(20),
	close BIGINT(20),
	diff BIGINT(20),
	volume BIGINT(20),
	PRIMARY KEY (code, date)
	)'''
            curs.execute(sql)
        self.conn.commit()

        self.codes = dict()
        self.update_comp_info()




    def __del__(self):
        '''소멸자: MariaDB 연결 해제'''
        self.conn.close()




    def read_krx_code(self): # 종목 코드 구하기
        '''KRX로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 반환'''
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns = {'종목코드':'code','회사명':'company'})

        krx.code = krx.code.map('{:06d}'.format)
        return krx





    def update_comp_info(self):
        '''종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장'''
        sql = 'select * from company_info'
        df = pd.read_sql(sql, self.conn)
 
        for idx in range(len(df)):
            

            self.codes[df['code'].values[idx]]=df['company'].values[idx]
        with self.conn.cursor() as curs:
            sql = 'select max(last_update) from company_info'
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0]==None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]

                    sql = f"replace into company_info (code, company, last_update) values ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} replace into company_info values({code}, {company}, {today})")
                self.conn.commit()
                print('')
   


    def read_naver(self, code, company, pages_to_fetch):
        '''네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 반환'''
        try:
            url = f"http//finance.naver.com/item/sise_day.nhn?code={code}"
            html = requests.get(url, headers = {'User-agent':'Mozilla/5.0'}).text
            bs = BeautifulSoup(html, 'lxml')
            pgrr = bs.find('td', class_='pgRR')
            if pgrr is None:
                return None
            s = str(pgrr.a['href']).split('=')
            lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages+1):
                url = '{}&page={}'.format(url, page)
                req = requests.get(u, headers={'User-agent': 'Mozilla/5.0'})
                df = df.append(pd.read_html(req.text, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end='\r')
            df = df.rename(columns = {'날짜':'date', '종가':'close', '전일비':
            'diff', '시가':'open', '고가':'high', '저가':'low', '거래량':'volumne'})
            df['date'] = df['date'].replace('.','-')
            df = df.dropna()
            df[['close', 'diff', 'open',' high', 'low', 'volume']] = df[['close', 'diff', 'open',' high', 'low', 'volume']].astype(int)
            df = df[['date', 'open','jigj', 'low', 'close', 'diff', 'volume']]
        except Exception as e:
            print('Exception occured :',str(e))
            return None
        return df


    def replace_into_db(self, df, num, code, company):
        '''네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE'''
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"replace into daily_price values ('{code}' ,'{r.date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.diff}, {r.volumne})"
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))
    
    
    def update_daily_price(self, pages_to_fetch):
        '''KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트'''
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])
    # def execute_daily(self):
    #     '''실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트'''

if __name__=='__main__':
    dbu = DBUpdater()
    dbu.update_comp_info()