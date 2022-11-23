"""
Inputs:
    - data: Source data in the form of dataframe
    - date_time_axis: X-axis variable(date-time format)
    - value: Y-axis variable
    - date_time_frequency: date-time frequency user want to specify('Months','Calendar Days','Weeks','Business Days','Hours','Minutes','Seconds','Years')
    - sql_filter: SQL queries for filter
    - value_type: Summary Statistic Type ("Count","Count Percentage","Unique Count","Average","Maximum","Minimum","Median","Total")
Output:
    - Data in the form of dataframe with summary table and plot
"""
import traceback
import pandas as pd
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        self.frequency_option_to_string = {
            'Months': 'M',
            'Calendar Days': 'D',
            'Weeks': 'W',
            'Business Days': 'B',
            'Hours': 'H',
            'Minutes': 'T',
            'Seconds': 'S',
            'Years': 'Y'
        }
        self.value_type_to_string = {
            'Count': 'count',
            'Count Percentage': 'count_percentage',
            'Unique Count': 'nunique',
            'Average': 'mean',
            'Maximum': 'max',
            'Minimum': 'min',
            'Median': 'median',
            'Total': 'sum'
        }

    def execute(self, data: pd.DataFrame, date_time_axis, value, date_time_frequency, sql_filter, value_type=None):
        value_type = value_type.strip()

        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        def count_percentage(x):

            return 100*x.count()/len(data[value])

        try:
            data = data_filter(data, sql_filter)
            data[date_time_axis] = pd.to_datetime(data[date_time_axis], infer_datetime_format=True)
            frequency = self.frequency_option_to_string.get(date_time_frequency)
            operation_type = self.value_type_to_string.get(value_type)
            data[date_time_axis] = data[date_time_axis].dt.to_period(frequency)
            if operation_type == 'count_percentage':
                table = data.groupby([date_time_axis]).agg({value: count_percentage
                                                            }).reset_index()
                table.columns = [date_time_frequency, value + ' ' + value_type]
                table[date_time_frequency] = table[date_time_frequency].astype(str)

                df_plot.bar_chart(value_type+' Of ' + value + ' For Different ' + date_time_frequency
                                  , table.columns[0], table.columns[1], table)
            else:
                table = data.groupby([date_time_axis]).agg({value: operation_type
                                                            }).reset_index()
                table.columns = [date_time_frequency, value + ' ' + value_type]
                table[date_time_frequency] = table[date_time_frequency].astype(str)
                df_plot.bar_chart(value_type+' Of ' + value + ' For Different ' + date_time_frequency
                                  , table.columns[0], table.columns[1], table)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
