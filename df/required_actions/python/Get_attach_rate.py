import traceback
from datetime import datetime, time, timedelta
import numpy as np
from dft.base_execution_handler import BaseExecutionHandler
import pandas as pd
    
class ExecutionHandler(BaseExecutionHandler):
    def execute(self, data, group_column, product_param, to_date_str, from_date_str = ''):
        
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d')
        if from_date_str:
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d')
        else:
            from_date = to_date-timedelta(days=1)
        try:
            data['bookingDate'] = pd.to_datetime(data['bookingDate'])
            data['total_premium'] = data['premiumAmount_onward'].add(data['premiumAmount_return'])
            data = data[(data['bookingDate']>=from_date)&(data['bookingDate']<to_date)]
            report = data.groupby(group_column).agg({'bookingId':'count','totalFare':'sum'}).rename(columns = {'bookingId':'Total_Bookings','totalFare':'ATV Total'})
            report[['Insured_Bookings','Avg_Premium','ATV Reschedule']] = data[data[product_param]==1].groupby(group_column).agg({'bookingId':'count', 'total_premium':'sum','totalFare':'sum'})
            report.loc['Total'] = report.sum(axis=0)
            report['Avg_Premium'] = report['Avg_Premium'].div(report['Insured_Bookings']).round(2)
            report['ATV Reschedule'] = report['ATV Reschedule'].div(report['Insured_Bookings']).round(2)
            report['ATV Total'] = report['ATV Total'].div(report['Total_Bookings']).round(2)
            report.fillna(0, inplace=True)
            report['Attach_Rate'] = (100*report['Insured_Bookings'].div(report['Total_Bookings']).round(2)).astype(str)+' %'
            report.iloc[:,0:3] = report.iloc[:,0:3].astype(int)

        except Exception as e:
            print(e)
            print(traceback.format_exc())
            raise
        print(report)
        return report.reset_index()