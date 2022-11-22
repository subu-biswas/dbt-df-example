import pandas as pd

'''
Inputs:
    - df: Dataframe on which actions are to be performed
    - sql: condition statement
Output:
    - Filtered Data set based on the above column filters
'''


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df: pd.DataFrame, sql):
        try:
            df = df.query(f'{sql}')
        except Exception as e:
            print("Malformed SQL. Filtering aborted")
        return df
