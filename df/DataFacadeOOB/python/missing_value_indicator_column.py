"""
Inputs:
    - data: Source data in the form of dataframe
Output:
    - Data in the form of dataframe with a new indicator column
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame):
        data['Missing_Value_Indicator'] = data.isna().any(axis=1)

        return data
