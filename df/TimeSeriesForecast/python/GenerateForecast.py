import pandas as pd
from dateutil.relativedelta import relativedelta
from pmdarima import auto_arima
from statsmodels.tsa.holtwinters import ExponentialSmoothing

'''
Inputs: 
    - df: Main dataframe whose indexes are timestamps
    - forecast_period_in_months: How many months of forecasted data you want
    - primary_dimension_values: A list of values of the primary dimension according to which the data set is already grouped by. These should be the columns of "df"
Output:
    - A dataframe whose last {forecast_period_in_months} rows are forecasted rows, while remaining are historical data.
'''
from dft import df_plot


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

    def execute(self, df: pd.DataFrame, forecast_period: int, frequency: str, period: int, sample_data_count):
        forecast_list = []

        frequency_code = self.frequency_option_to_string.get(frequency)
        frequency_type = str(period) + frequency_code
        df = df.asfreq(frequency_type)
        column_means = df.mean()
        df = df.fillna(column_means)
        primary_dimension_values = df.columns.tolist()
        trimmed_df = df.tail(min(sample_data_count, len(df)))
        for primary_dimension_value in primary_dimension_values:
            print("Executing " + primary_dimension_value)
            time_series = trimmed_df[primary_dimension_value].copy()

            forecast_model = auto_arima(time_series, start_p=0, start_q=0,
                                        test='adf',  # use adftest to find optimal 'd'
                                        max_p=3, max_q=3,  # maximum p and q
                                        start_Q=0,
                                        max_Q=5,
                                        m=12,  # frequency of series
                                        d=None,  # let model determine 'd'
                                        seasonal=True,  # No Seasonality
                                        start_P=0,
                                        D=0,
                                        trace=False,
                                        error_action='ignore',
                                        suppress_warnings=True,
                                        stepwise=True)
            forecast = pd.Series(forecast_model.predict(forecast_period),
                                 index=pd.date_range(time_series.index[-1] + self.get_relative_time_delta(frequency_code, 1), periods=forecast_period,
                                                     freq=frequency_type)).astype(int)
            forecast.name = time_series.name
            forecast_list.append(forecast)

        forecast_result = pd.concat(forecast_list, axis=1)
        forecast_result[forecast_result < 0] = 0
        forecast_df = df.append(forecast_result)[primary_dimension_values]

        df_plot.time_series_forecast("Time Series Forecast", forecast_period, forecast_df)

        return forecast_df

    def get_relative_time_delta(self, frequency_type, forecast_period):
        if frequency_type == 'MS':
            return relativedelta(months=forecast_period)
        if frequency_type == 'D':
            return relativedelta(days=forecast_period)
        if frequency_type == 'S':
            return relativedelta(seconds=forecast_period)
        if frequency_type == 'T':
            return relativedelta(minutes=forecast_period)
        if frequency_type == 'Y':
            return relativedelta(years=forecast_period)
        if frequency_type == 'B':
            return relativedelta(weekday=forecast_period)
        if frequency_type == 'W':
            return relativedelta(weeks=forecast_period)


