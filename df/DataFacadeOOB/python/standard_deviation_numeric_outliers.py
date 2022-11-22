"""
Inputs:
    - data: Source data in the form of dataframe
    - column: Column name for which you want to handel outlier
    - number_of_sd: The number of Standard deviations from which a value must vary from the mean (default 1)
    - fix_method: Method used to handle outliers when they are detected ('clip'/'remove'(default)/'invalidate')
Output:
    - Data in the form of dataframe with transformed column
"""
import numpy as np
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], number_of_sd=1, fix_method='remove'):
        df_column = data.columns.tolist()
        try:
            for column in columns:
                if column not in df_column:
                    continue
                sd = data[column].std()
                mean = data[column].mean()
                L1 = mean - number_of_sd * sd
                L2 = mean + number_of_sd * sd
                if fix_method == 'clip':
                    data[column] = data[column].apply(lambda x: L2 if x > L2 else (L1 if x < L1 else x))

                elif fix_method == 'remove':
                    data.drop(np.where((data[column] > L2) | (data[column] < L1))[0].tolist(), axis=0, inplace=True)
                    data = data.reset_index().drop('index', axis=1)

                elif fix_method == 'invalidate':
                    data[column] = data[column].apply(lambda x: np.nan if x > L2 else (np.nan if x < L1 else x))

        except Exception as e:
            raise type(e)(e)

        return data
