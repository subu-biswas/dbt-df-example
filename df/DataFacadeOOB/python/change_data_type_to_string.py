"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to change data type
Output:
    - Data in the form of dataframe with changed columns
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            for col in columns:
                data[col] = data[col].astype(str)

        except Exception as e:
            raise type(e)(e)

        return data
