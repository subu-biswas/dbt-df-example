"""
Action for calculating the vanilla RFM segments
Inputs:
    - rfm_table: Dataframe with RFM values
    - recency_column: Recency column of the rfm_table
    - frequency_column: Frequency column of the rfm_table
    - monetary_value_column: Monetary Value column of the rfm_table
Output:
    - Modified dataframe with RFM values and vanilla RFM segments
"""
import traceback
import numpy as np
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, rfm_table, recency_column, frequency_column, monetary_value_column):
        def rfm_score_segment(table, recency, frequency, monetary, n_segments=3):  # function for calculating vanilla RFM segments
            # Recency
            quantiles = table[recency].unique()
            quantiles = np.quantile(quantiles, q=[i / 3 for i in range(1, 4)]).tolist()

            def r_class(x, p=quantiles):
                if x <= p[0]:
                    return 3
                elif x <= p[1]:
                    return 2
                else:
                    return 1

            table['R'] = table[recency].apply(r_class)
            # Frequency
            quantiles = table[frequency].unique()
            quantiles = np.quantile(quantiles, q=[i / 3 for i in range(1, 4)]).tolist()

            def fm_class(x, p=quantiles):
                if x <= p[0]:
                    return 1
                elif x <= p[1]:
                    return 2
                else:
                    return 3

            table['F'] = table[frequency].apply(fm_class)

            # Monetary Value
            quantiles = table[monetary].unique()
            quantiles = np.quantile(quantiles, q=[i / 3 for i in range(1, 4)]).tolist()
            table['M'] = table[monetary].apply(fm_class)

            # Create RFM Segment & RFM Score
            def join_rfm(x):
                return str(x['R']) + str(x['F']) + str(x['M'])

            table['RFM_Score'] = table[['R', 'F', 'M']].sum(axis=1)
            table['RFM_Segment'] = table.R.map(str) + table.F.map(str) + table.M.map(str)

            def segment_name(t):  # functions for determining the segment names
                tmp = []
                for i in t['RFM_Segment']:
                    if i == '333':
                        tmp.append('Best Customer')
                    elif (i == '133') | (i == '123') | (i == '113'):
                        tmp.append('Lost Customer')
                    elif (i == '111') | (i == '211'):
                        tmp.append('Lost Cheap Customer')
                    elif (i == '233') | (i == '223'):
                        tmp.append('Almost Lost')
                    elif (i == '121') | (i == '131'):
                        tmp.append('Loyal Customer')
                    elif i == '311':
                        tmp.append('New Visitor')
                    elif (i == '313') | (i == '323'):
                        tmp.append('Big Spender')
                    else:
                        tmp.append('Others')

                return tmp

            table['Segment'] = segment_name(table)

            return table
        try:
            rfm_segment_table = rfm_score_segment(rfm_table, recency_column, frequency_column, monetary_value_column)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return rfm_segment_table
