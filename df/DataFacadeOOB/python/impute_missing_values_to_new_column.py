"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of columns for which imputation will be done
    - algorithm: Imputation algorithm (input can be either 'mean','median' and 'mode', default value is None)
Output:
    - Data in the form of dataframe with a new indicator column
"""
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], algorithm=None):
        df_columns = data.columns.tolist()
        for col in columns:
            if col not in df_columns:
                continue
            if algorithm == 'mean':
                if data[col].dtype == 'O':
                    raise ValueError('Algorithm not applicable on object type columns chose numeric columns')
                else:
                    data['new' + '_' + col] = data[col].fillna(data[col].mean())

            elif algorithm == 'median':
                if data[col].dtype == 'O':
                    raise ValueError('Algorithm not applicable on object type columns chose numeric columns')
                else:
                    data['new' + '_' + col] = data[col].fillna(data[col].median())

            elif algorithm == 'mode':
                data['new' + '_' + col] = data[col].fillna(data[col].value_counts().index[0])
            else:
                if (data[col].dtype == 'float') | (data[col].dtype == 'int'):
                    data['new' + '_' + col] = data[col].fillna(0)

                else:
                    data['new' + '_' + col] = data[col]

        return data
