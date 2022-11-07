"""
Action for converting source raw data to RFM model schema
Inputs:
    - Data: Fintech source data in the form of Dataframe

Output:
    - Dataframe in the form of RFM model schema
"""
import traceback
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler
from dateutil import parser


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, Data):

        try:
            table = Data
            table['first_open'] = [parser.parse(row_date) for row_date in table['first_open']]
            table["enrolled_date"] = [parser.parse(row_date) if isinstance(row_date, str) else row_date for
                                      row_date in table["enrolled_date"]]
            table['response_time'] = (table.enrolled_date - table.first_open).astype('timedelta64[h]')
            table.loc[table.response_time > 48, 'enrolled'] = 0
            table['hour'] = table.hour.str.slice(1, 3).astype(int)
            table = table.drop(columns=['enrolled_date', 'response_time', 'first_open'])
            return table
        except Exception as e:
            print(e)
            print(traceback.format_exc())
