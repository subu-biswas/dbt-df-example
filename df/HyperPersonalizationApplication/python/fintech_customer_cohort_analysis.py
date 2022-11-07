"""
Action for converting source raw data to RFM model schema
Inputs:
    - Data: Fintech source data in the form of Dataframe

Output:
    - Dataframe in the form of RFM model schema
"""
import traceback
import pandas as pd
import datetime as dt
from dft.base_execution_handler import BaseExecutionHandler
from dateutil import parser
from dft import df_plot


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, fintech_data, propensity_data, CustomerID):

        try:
            cohort = fintech_data.copy()

            def get_month(x):
                return dt.datetime(x.year, x.month, 1)

                # Create InvoiceMonth column

            cohort['first_open'] = pd.to_datetime(cohort['first_open'], errors='coerce', infer_datetime_format=True)
            cohort["enrolled_date"] = pd.to_datetime(cohort["enrolled_date"], errors='coerce', infer_datetime_format=True)
            cohort['CohortMonth'] = cohort['first_open'].apply(get_month)

            def get_date_int(df, column):
                year = df[column].dt.year
                month = df[column].dt.month
                return year, month

            enrolled_year, enrolled_month = get_date_int(cohort, 'enrolled_date')

            cohort_year, cohort_month = get_date_int(cohort, 'CohortMonth')

            years_diff = enrolled_year - cohort_year
            months_diff = enrolled_month - cohort_month
            cohort['CohortIndex'] = years_diff * 12 + months_diff + 1
            cohort = cohort.set_index(CustomerID)
            cohort = cohort.loc[propensity_data[CustomerID]]
            cohort = cohort.reset_index()
            cohort['Propensity'] = propensity_data['Propensity']
            grouping = cohort.groupby(['CohortMonth', 'CohortIndex'])
            cohort_data = grouping['Propensity'].mean()
            cohort_data = cohort_data.reset_index()
            average_price = cohort_data.pivot(index='CohortMonth', columns='CohortIndex', values='Propensity')
            average_price = average_price.round(1)
            average_price.index = average_price.index.date
            average_price = average_price.reset_index()
            df_plot.heat_map_dataframe('Customer Propensity By Monthly Cohort', average_price.columns[0],
                                       list(average_price.columns[1:]), average_price)

            return average_price
        except Exception as e:
            print(e)
            print(traceback.format_exc())
