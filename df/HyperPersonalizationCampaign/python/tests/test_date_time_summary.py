import pandas as pd

from df.app1.Python.src.date_time_summary import ExecutionHandler


def test_two_d_single_axis_date_time_summary_plot():
    execution_handler = ExecutionHandler()
    """
    -case1
    """
    Data = pd.DataFrame({'col1': ['01/01/2018', '01/09/2018', '01/05/2018', '01/03/2018', '02/01/2018', '08/02/2018',
                                  '12/05/2018', '15/03/2018'],
                         'col2': [10, 20, 10, 20, 30, 30, 40, 50]})
    date_time_axis = 'col1'
    value = 'col2'
    date_time_frequency = 'Months'
    sql_filter = 'optional'
    value_type = 'Total'
    result = execution_handler.execute(Data, date_time_axis, value, date_time_frequency, sql_filter, value_type)
    expected_result = pd.DataFrame({'Months': ['2018-01', '2018-02', '2018-03', '2018-08', '2018-12'],
                                    'col2 Total': [60, 30, 50, 30, 40]})
    pd.testing.assert_frame_equal(result, expected_result)
    """
       -case2
       """
    Data = pd.DataFrame({'col1': ['01/01/2018', '01/09/2018', '01/05/2018', '01/03/2018', '02/01/2018', '08/02/2018',
                                  '12/05/2018', '15/03/2018'],
                         'col2': [10, 20, 10, 20, 30, 30, 40, 50]})
    date_time_axis = 'col1'
    value = 'col2'
    date_time_frequency = 'Months'
    sql_filter = 'optional'
    value_type = 'Count Percentage'
    result = execution_handler.execute(Data, date_time_axis, value, date_time_frequency, sql_filter, value_type)
    expected_result = pd.DataFrame({'Months': ['2018-01', '2018-02', '2018-03', '2018-08', '2018-12'],
                                    'col2 Count Percentage': [50, 12.5, 12.5, 12.5, 12.5]})
    pd.testing.assert_frame_equal(result, expected_result)

