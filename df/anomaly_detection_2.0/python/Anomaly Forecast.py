#Forcast anomaly
import numpy as np
#import keras
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler
from prophet import Prophet
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import pandas as pd
import math
from dateutil.relativedelta import relativedelta
from pmdarima import auto_arima
from statsmodels.tsa.arima.model import ARIMA


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

    def execute(self, df: pd.DataFrame, forecast_period: int, dimension_column, timestamp_column, threshold_percentage, email):
        forecast_list = []

        frequency_type = pd.infer_freq(df[timestamp_column])
        df.sort_values(by=timestamp_column, ascending=True)
        # df = df.asfreq(frequency_type)
        column_means = df.mean()
        #df = df.fillna(method="ffill")

        primary_dimension_values = df.columns[1:]
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        df = df.set_index(timestamp_column)

        sample_data_count = 2000
        trimmed_df = df.tail(min(sample_data_count, len(df)))

        # ----fitting arima model and generating forcast for different dimension values
        file_paths = []
        images_paths = []
        tmp_df = pd.DataFrame()
        alert_count = 0
        for primary_dimension_value in primary_dimension_values:
            file_paths = []
            images_paths = []
            print("Executing " + primary_dimension_value)
            time_series = trimmed_df[primary_dimension_value].copy()
            target_value = trimmed_df[primary_dimension_value][trimmed_df.shape[0] - 1]
            if time_series[-1] == np.nan:
              print('The last observed value for '+primary_dimension_value+' is missing. Forecasting not possible.')
            else:
              pass
            print('Please Wait. Fitting Timeseries Model for ' + time_series.name)
            # ----fitting arima model
            # forecast_model = auto_arima(time_series, start_p=0, start_q=0,
            #                             test='adf',  # use adftest to find optimal 'd'
            #                             max_p=3, max_q=3,  # maximum p and q
            #                             start_Q=0,
            #                             max_Q=5,
            #                             m=12,  # frequency of series
            #                             d=None,  # let model determine 'd'
            #                             seasonal=True,  # No Seasonality
            #                             start_P=0,
            #                             D=0,
            #                             trace=False,
            #                             error_action='ignore',
            #                             suppress_warnings=True,
            #                             stepwise=True)
            model = ARIMA(time_series, order=(2, 1, 2))
            forecast_model = model.fit()
            print('Generating Forecast For ' + time_series.name)
            # forecast = pd.Series(forecast_model.predict(forecast_period),
            #                      index=pd.date_range(
            #                          start=trimmed_df.index.max(),
            #                          periods=forecast_period,
            #                          freq=frequency_type))
            forecast = pd.Series(forecast_model.forecast(forecast_period))
            forecast.name = time_series.name

            if sum(abs(forecast - target_value) / target_value > threshold_percentage/100 ) > 0:
                

                print('The Value Reached The Threshold')

                # plot the full timeseries
                plt.figure(figsize=(8, 4))
                plt.plot(time_series[-30:], '.-b', label='Actual Values')

                # plotting the anomaly
                plt.plot(forecast, 'rx--', linewidth=2, markersize=10,
                         label='Forecasted Values')

                # adding the threshold value
                plt.axhline(target_value + (target_value * threshold_percentage / 100), color='orange', linestyle='--',
                            label=str(threshold_percentage) + '% Threshold')
                plt.axhline(target_value - (target_value * threshold_percentage / 100), color='orange', linestyle='--')
                plt.legend()
                plt.title('Actual Values And Forecasted Values Of '+dimension_column+' '+ primary_dimension_value)
                plt.savefig("plot"+str(alert_count), bbox_inches="tight",
                            pad_inches=0.1, transparent=True)
                plt.close()
                # images_paths.append("/tmp/output.jpg")
                output_df = pd.DataFrame()
                output_df['Values'] = time_series[-min(100, len(time_series)):].append(forecast)
                is_forecast = [False] * min(100, len(time_series)) + [True] * forecast_period
                is_significant = [np.nan] * min(100, len(time_series)) + list(abs(forecast - target_value) / target_value > threshold_percentage / 100)
                output_df['Is Forecasted'] = is_forecast
                output_df['Absolute Difference In %'] = [np.nan] * min(100, len(time_series)) + list(100*abs(forecast - target_value) / target_value)
                output_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] = is_significant
                output_df[dimension_column] = [primary_dimension_value] * (min(100, len(time_series))+forecast_period)

                tmp_df = tmp_df.append(output_df)
                alert_count = alert_count + 1
                # 

                # 
                # 

            else:

                pass

            forecast_list.append(forecast)

        forecast_result = pd.concat(forecast_list, axis=1)
        forecast_result[forecast_result < 0] = 0
        forecast_df = df.append(forecast_result)[primary_dimension_values]

        # df_plot.time_series_forecast("Time Series Forecast", forecast_period, forecast_df)
        if alert_count > 0:
          fig, axs = plt.subplots(math.ceil(alert_count/2), 2, figsize=(80, 80))
          for i, ax in zip(range(0,alert_count),axs.flat):
            ax.set_axis_off()
            filename = 'plot' + str(i) + '.png'
            ax.imshow(mpimg.imread(filename))
          plt.savefig("/tmp/forecast.png")
          images_paths.append("/tmp/forecast.png")

          tmp_df.reset_index().to_csv("/tmp/output_df.csv")
          file_paths.append("/tmp/output_df.csv")
          df_helper.send_email(email, "Ixigo Anomaly Detection", "The Value Reached The Threshold", images_paths,
                               file_paths)

        else:
          tmp_df ='No anomaly detected'

        return tmp_df

    def get_relative_time_delta(self, frequency_type, forecast_period):
        if frequency_type == 'MS':
            return relativedelta(months=forecast_period)
        if frequency_type == 'H':
            return relativedelta(hours=forecast_period)
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
