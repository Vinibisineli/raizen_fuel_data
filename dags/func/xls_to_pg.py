#!/usr/bin/env python3

from tokenize import String
import requests
from sqlalchemy import create_engine

class ExcelToPG():

    def get_data_from_url(url: String, sheet_name: String, schema: String, table:String):
        
        # r = requests.get(url)
        # open('tmp_raizen.xls', 'wb').write(r.content)

        # df = pd.read_excel('tmp_raizen.xls', sheet_name = None)
        # #, sheet_name="DPCache_m3"

        df = pd.read_excel('./dags/files/vendas-combustiveis-m3.xls', sheet_name=sheet_name)
        print(df.keys())

        #creating database conection
        engine = create_engine('postgresql+psycopg2://raizen:raizen@postgres-raizen:5432/postgres')

        #inserting data
        table_data = schema + '.' + table
        df.to_sql(name=table, con=engine, if_exists='replace',index=False)




