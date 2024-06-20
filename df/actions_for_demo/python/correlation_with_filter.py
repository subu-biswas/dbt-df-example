
import traceback
import numpy as np
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, data: pd.DataFrame, column_x, column_y, sql_filter):

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        try:
            data = data_filter(data, sql_filter)
            correlation = round(data[column_x].corr(data[column_y]), 2)

            string = 'The Correlation Between '+ column_x + ' & '+ column_y + ' Is ' + str(correlation)


            df_plot.gauge_single_value('The Correlation Between '+ column_x + ' & '+ column_y, correlation, -1, 1)

            df_plot.scatter_chart(string, column_x, column_y, data, expose_data=True)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return string
