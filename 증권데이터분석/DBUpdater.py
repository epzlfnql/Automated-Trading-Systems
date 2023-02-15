import pymysql
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import calendar
import time
from threading import Timer
from sqlalchemy import create_engine


class DBUpdater:
    def __init__(self):
        """생성자 : MariaBD 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root', password='ghdtnsduf1',
                                    db='Investar', charset='utf8')

        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
            code VARCHAR(20),
            company VARCHAR(40),
            last_update DATE,
            PRIMARY KEY (code))
            """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price(
            code VARCHAR(20),
            date DATE,
            open BIGINT(20),
            high BIGINT(20),
            low BIGINT(20),
            close BIGINT(20),
             diff BIGINT(20),
            volume BIGINT(20),
            PRIMARY KEY (code, date))
            """
            curs.execute(sql)
        self.conn.commit()

        self.codes = dict()
        self.update_comp_info()




    def __del__(self):
        """소멸자 : MariaDB 연결 해제"""
        self.conn.close()




    def read_krx_code(self):
        """KRX로부터 상장법인목록파일을 읽어와서 데이터프레임으로 전환"""
        krx = pd.read_html("C:/Users/----/OneDrive/바탕 화면/FinanceAnalysisProject/상장법인목록.xls")[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx


    def read_total_data(url, last_page):
        total_data = pd.DataFrame()
        
        for page in range(0, int(last_page) + 1):
            data_per_page = read_per_page(url, page)
            total_data = total_data.append(data_per_page)
        return total_data



    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last"\
                        f"_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info "\
                        f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print('')
   


    def read_naver(self, code, company,pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 전환"""
        try:
            url = f'https://finance.naver.com/item/sise_day.nhn?code={code}&page=1'
            req = requests.get(url, headers={'User-agent': 'Mozilla/5.0'}).text

            html = BeautifulSoup(req, 'lxml')
            pgrr = html.find('td', class_='pgRR')

            parsed_url = str(pgrr.a['href']).split('=')
            last_page = parsed_url[-1]

            total_data = pd.DataFrame()
            sise_url = f'https://finance.naver.com/item/sise_day.nhn?code={code}'
            total_data = read_total_data(sise_url, last_page, pages_to_fetch)

            total_data = total_data[::-1]
            total_data = total_data.rename(columns={'시가':'Open','고가':'High','저가':'Low','종가':'Close','거래량':'Volume','전일비':'Diff'})
            total_data.index = pd.to_datetime(total_data.index)
            total_data = total_data.dropna()
            total_data["Date"] = total_data.index
            total_data = total_data.reset_index(drop=True)
            total_data[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']] = total_data[['Close', 'Diff', 'Open', 'High', 'Low', 'Volume']].astype(int)
            total_data = total_data[['Date','Close', 'Diff', 'Open', 'High', 'Low', 'Volume']]
        except Exception as e:
            print('Exception occured : ', str(e))
            return None
        return total_data

    def replace_into_db(self, df, num, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}', "\
                      f"'{r.Date}', {r.Open}, {r.High}, {r.Low}, {r.Close}, "\
                      f"{r.Diff}, {r.Volume})"
                curs.execute(sql)
        self.conn.commit()
        print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_' \
              'price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num + 1, company, code, len(df)))
    
    
    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code],pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])


    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트 """
        self.update_comp_info()
        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {"pages_to_fetch": 1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month ==12 and tmnow.day ==lastday:
            tmnext = tmnow.replace(year= tmnow.year+1, month=1, day=1,
            hour= 17, minute=0, second = 0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month = tmnow.month+1, day=1, hour=17, minute = 0, second = 0)

        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour = 17, minute=0, second = 0)

        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        t = Timer(secs, self.execute_daily)
        print('Waiting for next update ({}) ...'.format(tmnext.strftime('%Y-%m-%d %H: %M')))
        t.start()

if __name__=='__main__':
    dbu = DBUpdater()
    dbu.execute_daily()