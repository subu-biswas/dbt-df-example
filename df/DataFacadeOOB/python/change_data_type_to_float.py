"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of column names for which you want to change data type
Output:
    - Data in the form of dataframe with changed columns
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str]):
        try:
            for col in columns:
                if data[col].dtype == 'O':

                    if data[col].str.isnumeric().sum() < len(data[col]):

                        raise ValueError('The column data type should be numeric')

                        break

                    else:
                        data[col] = data[col].astype(float)
                else:
                    data[col] = data[col].astype(float)

        except Exception as e:

            raise type(e)(e)

        return data