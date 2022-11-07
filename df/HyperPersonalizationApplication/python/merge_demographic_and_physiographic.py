"""
Inputs:
    - df1: First Source data in the form of dataframe
    - df2: Second Source data in the form of dataframe
    - df1_key: Primary Key (column name) of the first source data
    - df2_key: Primary Key (column name) of the second source data
    - join_type: Types of join user wants to specify ('left'(default),'right',‘outer’,‘inner’,‘cross’)
Output:
    - Data in the form of dataframe with the moved column
"""
import traceback
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df1: pd.DataFrame, df2: pd.DataFrame, df1_key, df2_key, join_type='left'):
        try:
            df = pd.merge(df1, df2, how=join_type, left_on=df1_key, right_on=df2_key)
            df.drop(df2_key, axis=1, inplace=True)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return df


