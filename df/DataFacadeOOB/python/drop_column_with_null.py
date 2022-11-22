"""
Inputs:
    - data: Source data in the form of dataframe
    - null_percentage: Threshold value of the percentage of null value(default 90%)
Output:
    - Data in the form of dataframe without the columns with null values more than the threshold
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, null_percentage=90):
        for col in data.columns:
            if data[col].isna().sum() * 100 / len(data[col]) > null_percentage:
                data.drop(col, axis=1, inplace=True)

        return data
