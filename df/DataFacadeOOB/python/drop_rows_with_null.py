"""
Inputs:
    - data: Source data in the form of dataframe
    - null_percentage: Threshold value of the percentage of null value (default value 90%)
Output:
    - Data in the form of dataframe without the rows with null values more than the threshold
"""
import math
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, null_percentage=90):
        if null_percentage < 100:
            data.dropna(thresh=math.ceil(len(data.columns) * (1 - null_percentage / 100)), inplace=True)
            data = data.reset_index().drop('index', axis=1)

        elif null_percentage == 100:
            data.dropna(thresh=1, inplace=True)
            data = data.reset_index().drop('index', axis=1)

        return data