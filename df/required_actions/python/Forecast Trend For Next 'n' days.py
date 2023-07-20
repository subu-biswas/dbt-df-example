import pandas as pd
from dateutil.relativedelta import relativedelta
from pmdarima import auto_arima
import numpy as np
from dft.base_execution_handler import BaseExecutionHandler
from dft import df_plot
from prophet import Prophet


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, df: pd.DataFrame, forecast_period: int, timestamp_column, target_column, sql_filter):
        
        def data_filter(df: pd.DataFrame, sql):
            try:
                df = df.query(f'{sql}')
            except Exception as e:
                print("Malformed SQL. Filtering aborted")
                print(e)
            return df

        df = data_filter(df, sql_filter)

        frequency_type = pd.infer_freq(df[timestamp_column])
        df = df[[timestamp_column, target_column]]
        df.sort_values(by=timestamp_column, ascending=True)

        column_means = df.mean()
        df = df.fillna(column_means)

        trimmed_df = df.tail(min(3000, len(df)))

        time_series = trimmed_df[target_column].copy()
        # ----fitting prophet model
        print('Please Wait. Fitting Timeseries Model ')
        trimmed_df.rename(columns={timestamp_column: 'ds', target_column: 'y'}, inplace=True)
        model = Prophet(interval_width=0.95)

        # Training the model
        model.fit(trimmed_df)

        print('Generating Forecast')
        future = model.make_future_dataframe(periods=forecast_period, freq=frequency_type)
        print(model.predict(future))
        forecast = model.predict(future)[['ds', 'yhat']][-forecast_period:]
        forecast.rename(columns={'yhat': 'y'}, inplace=True)

        forecast_df = trimmed_df.append(forecast)
        forecast_df.rename(columns={'ds': timestamp_column, 'y': target_column}, inplace=True)
        forecast_df1 = forecast_df.set_index(timestamp_column)
        df_plot.time_series_forecast("Time Series Forecast", forecast_period, forecast_df1[-100:])

        return forecast_df

