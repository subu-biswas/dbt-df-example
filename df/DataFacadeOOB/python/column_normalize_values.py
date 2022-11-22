"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names on which you want to add transformation
Output:
    - Data in the form of dataframe with transformed columns
"""
import pandas as pd

from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        from sklearn.preprocessing import StandardScaler
        for col in columns:
            if (data[col].dtype == 'int') | (data[col].dtype == 'float'):
                scaler = StandardScaler().fit(data[[col]])
                data[col] = scaler.transform(data[[col]])

            else:
                raise ValueError('Data type of the column need to be integer or float.')

        return data
