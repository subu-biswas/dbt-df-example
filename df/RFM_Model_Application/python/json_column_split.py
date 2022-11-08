"""
Action for splitting columns , which are in .json format
Inputs:
    - data: Raw data in the form of Dataframe
    -json_columns: List of the json columns(list)
Output:
    - Modified data with split column in the form of dataframe
"""
import ast
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, json_columns: [str]):
        for i in json_columns:
            data[i] = data[i].apply(lambda x: ast.literal_eval(x))
            tmp = data[i].apply(pd.Series)
            tmp.columns = [f"{i}_{sub_column}" for sub_column in tmp.columns]
            data = pd.concat([data.drop([i], axis=1), tmp], axis=1)
        return data
