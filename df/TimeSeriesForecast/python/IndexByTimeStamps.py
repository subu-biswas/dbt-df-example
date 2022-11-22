import pandas as pd


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):

    def execute(self, df: pd.DataFrame, primary_dimension_column: str, target_columns: list, timestamp_column):
        date_processed = {}
        timeseries = pd.DataFrame()
        for index, row in df.iterrows():
            date = row[timestamp_column]
            if date in date_processed.keys():
                continue

            filtered_by_date = df.query(f'`{timestamp_column}` == "{date}"')
            date_processed[date] = 1
            row_config = {timestamp_column: date}
            for date_row_index, date_row in filtered_by_date.iterrows():
                for column in target_columns:
                    new_column_name = str(date_row[primary_dimension_column]) + "_" + str(column)
                    new_column_value = date_row[column]

                    row_config = {**row_config, new_column_name: new_column_value}

            timeseries = timeseries.append(row_config, ignore_index=True)

        timeseries = timeseries.set_index(timestamp_column)
        return timeseries
