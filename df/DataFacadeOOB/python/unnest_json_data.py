"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of  columns which are in json format ,need to be unnested
Output:
    - Data in the form of dataframe with the new separated columns
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        import ast
        try:
            for col in columns:
                data[col] = data[col].apply(lambda x: ast.literal_eval(x))

                temp = data[col].apply(pd.Series)
                temp.columns = [f"{col}_{subcolumn}" for subcolumn in temp.columns]

                data = pd.concat([data.drop([col], axis=1), temp], axis=1)

        except Exception as e:
            raise type(e)(e)

        return data

