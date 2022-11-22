"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names for which you want to generate correlation heat map
Output:
    - Correlation matrix and heat map
"""
import traceback
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            matrix = data[columns].corr()
            matrix = pd.DataFrame(matrix).reset_index()
            df_plot.heat_map_dataframe('Correlation Matrix', matrix.columns[0],
                                       list(matrix.columns[1:]), matrix)
            return matrix
        except Exception as e:
            raise type(e)(e)


