import pandas as pd


'''
Inputs:
    - df: Main data frame
    - primary_dimension: The column on which you want to generate the time forecast. This will be used as the primary and only dimension for now
    - timestamps: List of timestamps taken out from the data set column names in a generalised format for eg. %Y %M %d etc.
Output:
    - Gives a pivoted data set where timestamps are indexes and each column is a value of primary dimension
'''

from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df: pd.DataFrame, primary_dimension: str, timestamp_columns, aggregate_by):
        timestamps = []
        for column in timestamp_columns:
            if "/" in column:
                timestamps.append(column.split('/')[1])
            else:
                timestamps.append(column)

        date_col_dict = dict((i, i.split('/')[1]) for i in df.columns if "/" in i)
        df = df.rename(columns=date_col_dict)

        grouped_by = df.groupby(primary_dimension).agg({timestamp: aggregate_by for timestamp in timestamps}).reset_index()
        df = pd.melt(grouped_by, id_vars=[primary_dimension], value_vars=timestamps)

        pivoted_data = pd.pivot(df, values='value', index='variable', columns=primary_dimension)
        return pivoted_data
