import numpy as np
#import keras
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler
from prophet import Prophet
import matplotlib.pyplot as plt
import pandas as pd
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

    def execute(self, df: pd.DataFrame, observation_period: int, forecast_period: int, timestamp_column, threshold_percentage, email):
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
        for primary_dimension_value in primary_dimension_values:
            file_paths = []
            images_paths = []
            print("Executing " + primary_dimension_value)
            time_series = trimmed_df[primary_dimension_value].copy()
            target_value = trimmed_df[primary_dimension_value][trimmed_df.shape[0] - 1]
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

            # -----real time trend generation
            time_window = 3
            print('Please Wait. Generating Trend For ' + time_series.name)
            tmp_df = pd.DataFrame()
            tmp_df[primary_dimension_value] = trimmed_df[primary_dimension_value]
            tmp_df['trend'] = time_series.rolling(time_window).mean()
            tmp_df.fillna(method='bfill', inplace=True)
            tmp_df['Absolute Difference From Trend In %'] = 100 * abs(
                tmp_df[primary_dimension_value] - tmp_df['trend']) / tmp_df['trend']
            tmp_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] \
                = tmp_df['Absolute Difference From Trend In %'] > threshold_percentage
            tmp_df.rename(columns={primary_dimension_value: 'Actual Value (' + primary_dimension_value + ')',
                                   'trend': 'Trend Value (' + primary_dimension_value + ')'}, inplace=True)
            tmp_df = tmp_df[['Actual Value (' + primary_dimension_value + ')',
                             'Trend Value (' + primary_dimension_value + ')',
                             'Absolute Difference From Trend In %',
                             'Is Significant (Threshold Is ' + str(threshold_percentage) + '%)']]
            if sum(tmp_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'][
                   -observation_period:]) > 0:
                print('Real Time Value Significantly Deviating From Trend Values')
                fig = tmp_df[['Actual Value (' + primary_dimension_value + ')',
                              'Trend Value (' + primary_dimension_value + ')']][-min(30, len(time_series)):].plot(
                    marker='.', figsize=(12, 8))
                fig.get_figure().savefig("/tmp/trend_output.jpg", bbox_inches="tight",
                                         pad_inches=0.1, transparent=True)
                images_paths.append("/tmp/trend_output.jpg")
                tmp_df = tmp_df[-min(100, len(time_series)):]
                tmp_df.to_csv("/tmp/actual_vs_trend.csv")
                file_paths.append("/tmp/actual_vs_trend.csv")
                df_helper.send_email(email, "Ixigo Anomaly Detection",
                                     "Real Time Value Significantly Deviating From Trend Values", images_paths,
                                     file_paths)

            if sum(abs(forecast - target_value) / 100 > threshold_percentage / 100) > 0:
                file_paths = []
                images_paths = []

                print('The Value Reached The Threshold')

                # plot the full timeseries
                plt.figure(figsize=(12, 8))
                plt.plot(time_series[-30:], '.-b', label='Actual Values (' + primary_dimension_value + ')')

                # plotting the anomaly
                plt.plot(forecast, '--rx', linewidth=2, markersize=10,
                         label='Forecasted Values (' + primary_dimension_value + ')')

                # adding the threshold value
                plt.axhline(target_value + (target_value * threshold_percentage / 100), color='orange', linestyle='--',
                            label=str(threshold_percentage) + '% Threshold')
                plt.axhline(target_value - (target_value * threshold_percentage / 100), color='orange', linestyle='--')
                # plt.show()
                plt.legend()
                plt.savefig("/tmp/output.jpg", bbox_inches="tight",
                            pad_inches=0.1, transparent=True)
                images_paths.append("/tmp/output.jpg")
                output_df = pd.DataFrame()
                output_df['Values ('+primary_dimension_value+')'] = time_series[-min(100, len(time_series)):].append(forecast)
                is_forecast = [False] * min(100, len(time_series)) + [True] * forecast_period
                is_significant = [np.nan] * min(100, len(time_series)) + list(abs(forecast - target_value) / 100 > threshold_percentage / 100)
                output_df['Is Forecasted'] = is_forecast
                output_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'] = is_significant
                output_df.to_csv("/tmp/output_df.csv")

                file_paths.append("/tmp/output_df.csv")
                df_helper.send_email(email, "Ixigo Anomaly Detection", "The Value Reached The Threshold", images_paths,
                                     file_paths)

            else:

                pass

            forecast_list.append(forecast)

        forecast_result = pd.concat(forecast_list, axis=1)
        forecast_result[forecast_result < 0] = 0
        forecast_df = df.append(forecast_result)[primary_dimension_values]

        # df_plot.time_series_forecast("Time Series Forecast", forecast_period, forecast_df)

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
