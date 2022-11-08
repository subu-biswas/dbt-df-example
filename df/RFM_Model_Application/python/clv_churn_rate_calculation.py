"""
Action for calculating CLV,Churn Rate,Expected Number of Purchase by the Customer
Inputs:
    - rfm_table: Dataframe with RFM values
    - customer_id_column: Customer identification column of the rfm_table
    - recency_column: Recency column of the rfm_table
    - frequency_column: Frequency column of the rfm_table
    - monetary_value_column: Monetary Value column of the rfm_table
    - time_first_interaction: Column corresponds to time since first user interaction of the rfm_table
     -Period: Timeframe in which user wants the customer CLV and Churn rate
     -PenalizeCoefficient: Tuning parameter for CLV prediction model
Output:
    - Dataframe with RFM values,churn rate,clv and expected number of purchase
"""
import traceback
from dft import df_plot
import pandas as pd
from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, rfm_table, customer_id_column,  recency_column, frequency_column, monetary_value_column, time_first_interaction, Period, PenalizeCoefficient):

        def clv_churn_rate_calculation(table, id_customer, recency, frequency, monetary_value, t_first_interaction, period, penalize_coefficient):
            from lifetimes import BetaGeoFitter, GammaGammaFitter
            btyd = pd.DataFrame()
            alpha = penalize_coefficient
            if (any(table[recency] == 0) is True) or (any(table[frequency] == 0) is True) or (
                    any(table[monetary_value] == 0) is True) or (any(table[t_first_interaction] == 0) is True):
                print("Calculation is not possible, Recency,Frequency,Monetary Value and value of T must be > 0 ")
                raise

            else:
                try:
                    btyd[id_customer] = table[id_customer]
                    bgf = BetaGeoFitter(penalizer_coef=alpha)
                    bgf.fit(table[frequency], table[recency], table[t_first_interaction])
                    # Calculating Expected Number of Purchase For 180 days or 6 months
                    btyd['Expected Number of Purchase'] = bgf.predict(period * 30, table[frequency], table[recency],
                                                                      table[t_first_interaction]).astype(int)
                    btyd['Churn Rate'] = 1 - bgf.conditional_probability_alive(table[frequency], table[recency],
                                                                               table[t_first_interaction])
                    ggf = GammaGammaFitter(penalizer_coef=alpha)
                    ggf.fit(table[frequency], table[monetary_value])
                    btyd["Expected Average Spend"] = ggf.conditional_expected_average_profit(table[frequency],
                                                                                             table[monetary_value])
                    btyd["CLV"] = ggf.customer_lifetime_value(bgf,
                                                              table[frequency],
                                                              table[recency],
                                                              table[t_first_interaction],
                                                              table[monetary_value],
                                                              time=period,  # 6 month (period)
                                                              freq="D",  # frequency of T
                                                              discount_rate=0.01)
                except Exception as e:
                    print(e)
                    print(traceback.format_exc())
                    raise

            return btyd

        clv_table = clv_churn_rate_calculation(rfm_table, customer_id_column,  recency_column, frequency_column,
                                               monetary_value_column, time_first_interaction, Period, PenalizeCoefficient)
        table1 = pd.DataFrame(clv_table['CLV'].value_counts(bins=6, sort=False)).reset_index()
        table1.columns = ['CLV', 'Frequency']
        table1['CLV'] = table1['CLV'].astype(str)
        df_plot.line_chart('Distribution Of Customer Lifetime Value', table1.columns[0], table1.columns[1], table1)

        return clv_table
