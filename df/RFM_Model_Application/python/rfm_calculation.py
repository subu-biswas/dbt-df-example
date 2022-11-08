"""
Action for calculating Recency,Frequency,Monetary/Engagement values
 Inputs:
 -rfm_data: Dataframe with RFM model schema
 -id_customer: column names to be provided by user(string)
 -ts_interaction: column names to be provided by user(should be in proper date-time format)
 -id_interaction: column names to be provided by user(string)
 -n_interaction_value: column names to be provided by user(string)

 Output: Dataframe with Recency,Frequency,Monetary/Engagement  values
"""
import traceback
from datetime import date
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, rfm_data, id_customer, ts_interaction, id_interaction, n_interaction_value):
        try:
            ts_reference = pd.to_datetime(date.today())
            today = pd.to_datetime(date.today())
            # Calculation of RFM values
            table = rfm_data.groupby(id_customer).agg(
                {ts_interaction: [lambda d: (ts_reference - d.max()).days,  # Recency
                                  lambda d: (today - d.min()).days],
                 id_interaction: lambda num: num.count(),  # Frequency
                 n_interaction_value: [lambda value: abs(value).min(),  # Monetary
                                       lambda value: abs(value).mean(),
                                       lambda value: abs(value).max(),
                                       lambda value: abs(value).median()
                                       ]})
            table.columns = table.columns.droplevel(0)
            table.columns = ['Recency', "T", 'Frequency', "Monetary.min", "Monetary.avg", "Monetary.max", "Monetary"]
            table["Frequency"] = table["Frequency"].astype(int)
            table = table.reset_index()
            table.dropna(inplace=True)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return table
