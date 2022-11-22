"""
Inputs:
    - data: Source data in the form of dataframe
Output:
    - Data in the form of dataframe with shuffled row
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame):
        data = data.sample(random_state=10, frac=1)

        return data
