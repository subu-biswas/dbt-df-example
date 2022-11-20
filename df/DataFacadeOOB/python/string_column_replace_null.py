import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df: pd.DataFrame, column_names, replacement_value):

        df_columns = df.columns.tolist()
        for column_name in column_names:
            if column_name in df_columns:
                df[column_name].fillna(replacement_value, inplace = True)
        return df
