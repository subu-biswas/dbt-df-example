"""
Inputs:
    - data: Source data in the form of dataframe
    - frequency: Frequency type(Month,Calendar Day,Weekly,Business Day,Hourly,Minutes,Seconds,Yearly)
    - period: Period of the frequency type.
    - resample_method: Resample method user want to use (Up-sample(default),Down-sample)
Output:
    - Resampled Data in the form of dataframe
"""
import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def __init__(self):
        self.frequency_option_to_string = {
            'Month': 'MS',
            'Calendar Day': 'D',
            'Weekly': 'W',
            'Business Day': 'B',
            'Hourly': 'H',
            'Minutes': 'T',
            'Seconds': 'S',
            'Yearly': 'Y'
        }

    def execute(self, data: pd.DataFrame, frequency, time_stamp_format: str, period=1, resample_method="Up-sample"):
        try:
            if time_stamp_format != "Infer":
                data.index = pd.to_datetime(data.index, format=time_stamp_format)
            else:
                data.index = pd.to_datetime(data.index, infer_datetime_format=True)
            data = data.sort_index()
            frequency_type = self.frequency_option_to_string.get(frequency)
            frequency_type = str(period) + frequency_type
            if resample_method == "Up-sample":
                data = data.resample(frequency_type).ffill()
            else:
                data = data.resample(frequency_type).mean().dropna()
        except Exception as e:
            raise type(e)(e)

        return data
