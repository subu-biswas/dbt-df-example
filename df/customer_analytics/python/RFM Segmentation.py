import pandas as pd 
import traceback
from datetime import date
import numpy as np

df = df_helper.get_table(parameter_name="input_table", parameter_display_name="Input Table", parameter_description="Input Raw Table") 
id_customer = df_helper.get_column_list(parameter_name="id_customer", parameter_display_name="Customer ID", parameter_description=" Customer ID is the column(s) in the data set with customer identification number, phone number or email id that uniquely identifies the customer.")
id_interaction = df_helper.get_column("id_interaction","Interaction ID","The column with the interaction identification number or interaction date time stamp that uniquely identifies the purchase.")
ts_interaction = df_helper.get_column("ts_interaction","Interaction Date"," The column with the interaction date i.e date that was captured by the system when the customer made the interaction.")
n_interaction_value = df_helper.get_column("n_interaction_value","Interaction Value","The column with the interaction values that lists the interaction amount the customer for each interaction.")

try:
    #Schema Creation
    table = pd.DataFrame()
    table[id_customer] = df[id_customer]
    table[[ts_interaction, n_interaction_value]] = df[[ts_interaction, n_interaction_value]]
    table[id_interaction] = df[id_interaction]
    table[n_interaction_value] = table[n_interaction_value].astype(float)
    table[ts_interaction] = pd.to_datetime(table[ts_interaction], infer_datetime_format=True)
    for i in id_customer:
        print(i+' Is Taken As id_customer For The Model Schema')
    print(id_interaction + ' Is Taken As id_interaction For The Model Schema')
    print(ts_interaction + ' Is Taken As ts_interaction For The Model Schema')
    print(n_interaction_value + ' Is Taken As n_interaction_value For The Model Schema')

    # RFM Calculation
    ts_reference = pd.to_datetime(date.today())
    today = pd.to_datetime(date.today())    
    # Calculation of RFM values
    table = table.groupby(id_customer).agg(
        {ts_interaction: [lambda d: (ts_reference - d.max()).days,  # Recency
                          lambda d: (today - d.min()).days],
         id_interaction: lambda num: num.count(),  # Frequency
         n_interaction_value: [  # Monetary
                               lambda value: abs(value).mean()
                              ]})
    table.columns = table.columns.droplevel(0)
    table.columns = ['Recency', "T", 'Frequency', "Monetary"]
    table["Frequency"] = table["Frequency"].astype(int)
    table = table.reset_index().dropna()

    #RFM Segmentation

    def rfm_score_segment(table, recency, frequency, monetary, n_segments=4): # function for calculating vanilla RFM segments
         # Recency
         import numpy as np
         quantiles = table[recency].unique()
         quantiles = np.quantile(quantiles, q=[i / 4 for i in range(1, 5)]).tolist()

         def r_class(x, p=quantiles):
             if x <= p[0]:
                 return 4
             elif x <= p[1]:
                 return 3
             elif x <= p[2]:
                 return 2
             else:
                 return 1

         table['R'] = table[recency].apply(r_class)
        # Frequency
         quantiles = table[frequency].unique()
         quantiles = np.quantile(quantiles, q=[i / 4 for i in range(1, 5)]).tolist()

         def fm_class(x, p=quantiles):
            if x <= p[0]:
                return 1
            elif x <= p[1]:
                return 2
            elif x <= p[2]:
                return 3
            else:
                return 4

         table['F'] = table[frequency].apply(fm_class)

        # Monetary Value
         quantiles = table[monetary].unique()
         quantiles = np.quantile(quantiles, q=[i / 4 for i in range(1, 5)]).tolist()
         table['M'] = table[monetary].apply(fm_class)
        # Create RFM Segment & RFM Score
         def join_rfm(x):
            return str(x['R']) + str(x['F']) + str(x['M'])

         table['rfm_score'] = table[['R', 'F', 'M']].sum(axis=1)
         table['rfm_segment'] = table.R.map(str) + table.F.map(str) + table.M.map(str)

         return table

    table = rfm_score_segment(table, "Recency", "Frequency", "Monetary")
    def assign_customer_segment(df):
        def get_segment(r, f, m):
            if r == 4 and f == 4 and m == 4:
                return 'Champions'
            elif r <= 2 and f >= 3 and m >= 3:
                return 'Loyal Customers'
            elif r == 2 and f < 3 and m >= 3:
                return 'Big Spenders'
            elif r <= 2 and f >= 3 and m < 3:
                return 'Potential Loyalists'
            elif r < 2 and f < 3 and m <= 4:
                return 'New Customers'
            elif r >= 3 and f >= 3 and m >= 3:
                return 'Customers Needing Attention'
            elif r > 3 and f < 3 and m >= 3:
                return 'Nearly Lost'
            elif r > 2 and f >= 3 and m < 3:
                return 'Promising'
            elif r > 2 and f < 3 and m < 3:
                return 'At Risk'
            else:
                return 'Other'

        df['customer_segment'] = df.apply(lambda row: get_segment(row['R'], row['F'], row['M']), axis=1)
        return df
    table = assign_customer_segment(table)
except Exception as e:
    print(e)
    print(traceback.format_exc())

df_helper.publish(table)