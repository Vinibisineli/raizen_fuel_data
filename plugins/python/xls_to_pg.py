#!/usr/bin/env python3

import requests
import pandas as pd
from sqlalchemy import create_engine

class ExcelToPG():

    def get_data():
        url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
        r = requests.get(url)
        open('tmp_raizen.xls', 'wb').write(r.content)
        df = pd.read_excel('tmp_raizen.xls', sheet_name = None)
        #, sheet_name="DPCache_m3"
        df = pd.read_excel('./dags/files/vendas-combustiveis-m3.xls', sheet_name = "DPCache_m3")
        print(df.keys())


        engine = create_engine('postgresql+psycopg2://raizen:raizen@localhost:5432/postgres')
        df.to_sql(name='public.raizen_oil', con=engine, if_exists='append',index=False)


ExcelToPG.get_data()