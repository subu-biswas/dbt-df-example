"""
Inputs:
    - data: Source data in the form of dataframe
    - columns: List of columns for which numeric imputation need to be done
    - imputation_method: User need to specify the imputation method (Remove means removing the missing values from the data
                                                                     User should use Mean,Median or Mode if they want to impute
                                                                     the missing values using the mean , median or mode of the
                                                                     corresponding numeric columns)
Output:
    - Data in the form of dataframe with imputed columns
"""
import numpy as np
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, columns: [str], imputation_method='Remove'):

        df_columns = data.columns.tolist()
        try:
            for col in columns:
                if col in df_columns:
                    if imputation_method == 'Remove':

                        remove_row = data[col].isna()
                        remove_row = np.where(remove_row)[0]
                        remove_row = remove_row.tolist()
                        if len(remove_row) > 0:
                            data.drop(remove_row, axis=0, inplace=True)
                            data = data.reset_index().drop('index', axis=1)

                    elif imputation_method == 'Mean':

                        if "float" in str(data[col].dtype):
                            data[col].fillna(data[col].mean().round(2), inplace=True)
                        else:
                            data[col].fillna(int(data[col].mean()), inplace=True)

                    elif imputation_method == 'Mode':

                        data[col].fillna(data[col].mode()[0], inplace=True)

                    else:
                        if "float" in str(data[col].dtype):
                            data[col].fillna(data[col].median().round(2), inplace=True)
                        else:
                            data[col].fillna(int(data[col].median()), inplace=True)

                else:
                    pass

        except Exception as e:
            raise type(e)(e)

        return data
