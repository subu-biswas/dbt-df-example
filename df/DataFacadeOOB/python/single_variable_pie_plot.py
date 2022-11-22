"""
Inputs:
    - data: Source data in the form of dataframe
    - axis: X-axis variable
    - sql_filter: SQL queries for filter
    - summary_type: Summary Statistic Type ("Count","Count Percentage")
Output:
    - Data in the form of dataframe with summary table and pie plot
"""
import traceback
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data: pd.DataFrame, axis, sql_filter, summary_type=None):
        summary_type = summary_type.strip()

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        try:
            data = data_filter(data, sql_filter)
            if summary_type == 'Count':
                table = pd.DataFrame(data[axis].value_counts()).reset_index()
                table.columns = [axis, 'Count']

                df_plot.pie_chart('Count For ' + axis, table.columns[0], table.columns[1], table)

            else:
                table = pd.DataFrame(data[axis].value_counts(normalize=True)).reset_index()
                table.columns = [axis, 'Count Percentage']
                table['Count Percentage'] = 100 * table['Count Percentage']
                df_plot.pie_chart('Percentage Count For ' + axis, table.columns[0], table.columns[1], table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
