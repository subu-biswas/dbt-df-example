#Real Time Anomaly Detection
import math
import numpy as np
#import keras
from dft import df_plot
from dft.base_execution_handler import BaseExecutionHandler
from prophet import Prophet
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import pandas as pd
from dateutil.relativedelta import relativedelta


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

    def execute(self, df: pd.DataFrame, observation_period: int, dimension_column, timestamp_column, threshold_percentage, email):
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
        output_df = pd.DataFrame()
        alert_count = 0
        file_paths = []
        images_paths = []
        alert_list = []
        for primary_dimension_value in primary_dimension_values:
            
            print("Executing " + primary_dimension_value)
            time_series = trimmed_df[primary_dimension_value].copy()
            target_value = trimmed_df[primary_dimension_value][trimmed_df.shape[0] - 1]

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
            tmp_df.rename(columns={primary_dimension_value: 'Actual Values',
                                   'trend': 'Trend Values'}, inplace=True)
            tmp_df[dimension_column] = [primary_dimension_value]*len(time_series)

            # temporary variable to count number of categories for which alert generated

            
            
            if sum(tmp_df['Is Significant (Threshold Is ' + str(threshold_percentage) + '%)'][
                   -observation_period:]) > 0:
                print('Real Time Value Significantly Deviating From Trend Values')
                #fig, ax = plt.subplots()
                alert_list = alert_list + [primary_dimension_value]
                figure = tmp_df[['Actual Values' ,
                                 'Trend Values']][-min(30, len(time_series)):].plot(
                    marker='.', figsize=(8, 4), title = 'Actual Values VS Trend Values Of '+dimension_column+' '+ primary_dimension_value)
                figure.get_figure().savefig('plot'+str(alert_count), bbox_inches="tight", transparent=True)
                plt.close()

                tmp_df = tmp_df[-min(100, len(time_series)):]
                output_df = output_df.append(tmp_df)
                alert_count = alert_count+1
            else:
              pass

        if alert_count > 0:
          fig, axs = plt.subplots(math.ceil(alert_count/2), 2, figsize=(80, 80))
          for i, ax in zip(range(0,alert_count),axs.flat):
            ax.set_axis_off()
            filename = 'plot' + str(i) + '.png'
            ax.imshow(mpimg.imread(filename))
          plt.savefig("/tmp/trend_output.png")
          images_paths.append("/tmp/trend_output.png")
          output_df.reset_index().to_csv("/tmp/actual_vs_trend.csv")
          file_paths.append("/tmp/actual_vs_trend.csv")
          string = 'Anomaly detected in real time for the following ' + dimension_column + '\n'
          for i,j in zip(range(1,len(alert_list)+1), alert_list):
              string = string+ str(i)+ '. '+ j+ '\n' 
          df_helper.send_email(email, "Ixigo Anomaly Detection: Trend vs RealTime",
                               string, images_paths,file_paths)

        else:
          output_df = 'No anomaly detected'

        return output_df

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
