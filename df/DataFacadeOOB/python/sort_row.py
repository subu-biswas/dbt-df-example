"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Name of the column with respect to which sorting will be done
    - ascending: Sorting order(True(default value)/False)
Output:
    - Data in the form of dataframe with the sorted rows
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, column, ascending=True):

        try:
            if not ascending:
                data = data.sort_values(column, ascending=False)
            else:
                data = data.sort_values(column, ascending=True)

            data = data.reset_index().drop('index', axis=1)

        except Exception as e:
            raise type(e)(e)

        return data
