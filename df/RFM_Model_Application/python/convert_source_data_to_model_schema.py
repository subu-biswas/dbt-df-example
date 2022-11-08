"""
Action for converting source raw data to RFM model schema
Inputs:
    - Data: Source data in the form of Dataframe
    -CustomerID: column names to be provided by user(string)
    -Interaction_ID: column names to be provided by user(string)
    -Interaction_Date: column names to be provided by user(string)
    -Interaction_Value: column names to be provided by user(string)

Output:
    - Dataframe in the form of RFM model schema
"""
import traceback
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, Data, CustomerID, Interaction_ID, Interaction_Date, Interaction_Value):

        try:
            table = pd.DataFrame()
            table[CustomerID] = Data[CustomerID]
            table_columns = ['ts_interaction', 'n_interaction_value']
            table[table_columns] = Data[[Interaction_Date, Interaction_Value]]
            table['id_interaction'] = Data[Interaction_ID]
            table['n_interaction_value'] = table['n_interaction_value'].astype(float)
            for i in CustomerID:
                print(i+' Is Taken As id_customer For The Model Schema')
            print(Interaction_ID + ' Is Taken As id_interaction For The Model Schema')
            print(Interaction_Date + ' Is Taken As ts_interaction For The Model Schema')
            print(Interaction_Value + ' Is Taken As n_interaction_value For The Model Schema')
        except Exception as e:
            print(e)
            print(traceback.format_exc())

        return table
