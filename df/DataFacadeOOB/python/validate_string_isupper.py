"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add string format operation
Output:
    - Data in the form of dataframe with the indicator column showing if the string is in upper case
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns=None):
        try:
            for i in range(len(columns)):
                data[columns[i]] = data[columns[i]].astype(str)
                data[columns[i] + '_valid_isupper'] = data[columns[i]].str.isupper()

        except Exception as e:
            raise type(e)(e)

        return data
