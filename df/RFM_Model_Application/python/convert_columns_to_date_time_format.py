"""
Action for converting source raw data to RFM model schema
Inputs:
    - data: Raw data in the form of Dataframe
    -date_columns: List of the columns to be converted to date-time format(list)

Output:
    - Modified data with converted column to date-time format in the form of dataframe
"""
import traceback
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, date_columns: [str]):
        try:
            for i in date_columns:
                if data[i].dtypes == 'int64':
                    data[i] = data[i].astype(str)
                    data[i] = pd.to_datetime(data[i], errors='coerce')
                    print('Date-Time Format Of '+i+' Is Changed')
                else:
                    data[i] = pd.to_datetime(data[i], errors='coerce')
                    print('Date-Time Format Of ' + i + ' Is Changed')
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise
        return data
