"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Name of the column to be moved
    - index: Reference index in which the column will be moved (values: 'start','end' or integers<total number of columns)
      default value is 'start'
Output:
    - Data in the form of dataframe with the moved column
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, index='start'):
        if index == 'start':
            column_to_move = data.pop(column)
            data.insert(0, column, column_to_move)
        elif index == 'end':
            column_to_move = data.pop(column)
            data.insert(len(data.columns), column, column_to_move)
        else:
            if int(index) < len(data.columns):
                column_to_move = data.pop(column)
                data.insert(int(index) - 1, column, column_to_move)
            else:
                raise ValueError('Index value must be less than total number of columns')

        return data
