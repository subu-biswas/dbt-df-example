"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Name of the column to be moved
    - reference_column: Reference column with respect to which the column will be moved
    - position: Position in which the column will move wrt the reference column ('Before'(default)/'After')
Output:
    - Data in the form of dataframe with the moved column
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, reference_column, position='Before'):
        if position == 'Before':
            column_to_move = data.pop(column)
            loc = data.columns.get_loc(reference_column)
            data.insert(loc, column, column_to_move)
        elif position == 'After':
            loc = data.columns.get_loc(reference_column)
            if loc + 1 < len(data.columns):
                column_to_move = data.pop(column)
                data.insert(loc + 1, column, column_to_move)
            else:
                column_to_move = data.pop(column)
                data.insert(loc, column, column_to_move)
        else:
            raise ValueError('position can be either Before or After')

        return data
