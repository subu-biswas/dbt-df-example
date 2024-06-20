import traceback
from datetime import datetime, time, timedelta
import numpy as np
from dft.base_execution_handler import BaseExecutionHandler
import pandas as pd
    
class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, group_column, product_param,to_date_str,from_date_str=''):
        
        try:
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
            if from_date_str:
                from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
            else:
                from_date = to_date-timedelta(days=1)

            data['bookingDate']=pd.to_datetime(data['bookingDate']).dt.round(freq='D')
            data = data[(data['bookingDate']>=from_date)&(data['bookingDate']<=to_date)]
            data['total_premium'] = data['premiumAmount_onward'].add(data['premiumAmount_return'])
            new_df = data.groupby(group_column).agg({'bookingId':'count','totalFare':'sum'}).rename(columns = {'bookingId':'Total_Bookings','totalFare':'ATV Total'})
            new_df[['Insured_Bookings','Avg_Premium','ATV Reschedule']] = data[data[product_param]==1].groupby(group_column).agg({'bookingId':'count', 'total_premium':'sum','totalFare':'sum'})
            new_df['Avg_Premium'] = new_df['Avg_Premium'].div(new_df['Insured_Bookings']).round(2)
            new_df['ATV Reschedule'] = new_df['ATV Reschedule'].div(new_df['Insured_Bookings']).round(2)
            new_df['ATV Total'] = new_df['ATV Total'].div(new_df['Total_Bookings']).round(2)
            new_df.fillna(0, inplace=True)
            new_df['Attach_Rate'] = (100*new_df['Insured_Bookings'].div(new_df['Total_Bookings']).round(2))
            new_df.iloc[:,0:3] = new_df.iloc[:,0:3].astype(int)
            new_df=new_df.reset_index()

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise

        return new_df