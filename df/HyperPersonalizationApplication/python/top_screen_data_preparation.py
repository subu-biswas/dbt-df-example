"""
Action for converting source raw data to RFM model schema
Inputs:
    - data: Source data in the form of Dataframe
    -top_screen_data: List of screens in the form of dataframe

Output:
    - Dataframe in the form of RFM model schema
"""
import traceback
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, top_screen_data):

        try:
            table = data
            table['screen_list'] = table.screen_list.astype(str) + ','
            top_screens = top_screen_data['top_screens']
            for sc in top_screens:
                table[sc] = table.screen_list.str.contains(sc).astype(int)
                table['screen_list'] = table.screen_list.str.replace(sc + ",", "")
            table['other'] = table.screen_list.str.count(",")
            table.drop(columns=['screen_list'], inplace=True)
            savings_screens = ["Saving1", "Saving2", "Saving2Amount", "Saving4",
                               "Saving5", "Saving6", "Saving7", "Saving8", "Saving9",
                               "Saving10"]
            table["Savings_count"] = table[savings_screens].sum(axis=1)
            table = table.drop(columns=savings_screens)
            cm_screens = ["Credit1", "Credit2", "Credit3", "Credit3Container", "Credit3Dashboard"]
            table["CM_count"] = table[cm_screens].sum(axis=1)
            table = table.drop(columns=cm_screens)
            cc_screens = ["CC1", "CC1Category", "CC3"]
            table["CC_count"] = table[cc_screens].sum(axis=1)
            table = table.drop(columns=cc_screens)
            loan_screens = ["Loan", "Loan2", "Loan3", "Loan4"]
            table["Loan_count"] = table[loan_screens].sum(axis=1)
            table = table.drop(columns=loan_screens)
            print(table)

            return table
        except Exception as e:
            print(e)
            print(traceback.format_exc())
