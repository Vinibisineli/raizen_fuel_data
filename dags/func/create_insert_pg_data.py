#!/usr/bin/env python3

from tokenize import String
import json, sys
from psycopg2 import connect, Error

class CreateInsertData():

    def create_insert_data(json_file: String, sql_file: String, table_name: String):

        with open(json_file) as json_data:
            # use load() rather than loads() for JSON files
            record_list = json.load(json_data)

        if type(record_list) == list:
            first_record = record_list[0]
            columns = list(first_record.keys())


        sql_string = f'INSERT INTO {table_name} '
        sql_string += "(" + ', '.join(columns) + ")\nVALUES "    

        # enumerate over the record
        for record_dict in record_list:

            # iterate over the values of each record dict object
            values = []
            for col_names, val in record_dict.items():

                # Postgres strings must be enclosed with single quotes
                if type(val) == str:
                    # escape apostrophies with two single quotations
                    val = val.replace("'", "''")
                    val = "'" + val + "'"
                if val is None or val == 'None':
                    val = 'NULL'
                values += [ str(val) ]

            # join the list of values and enclose record in parenthesis
            sql_string += "(" + ', '.join(values) + "),\n"

        # remove the last comma and end statement with a semicolon
        sql_string = sql_string[:-2] + ";"

        with open(f"/opt/airflow/plugins/sql/{sql_file}.sql", "w+") as sql_str:
            sql_str.write(sql_string)
