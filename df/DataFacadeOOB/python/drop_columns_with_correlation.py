

from dft.base_execution_handler import BaseExecutionHandler



class ExecutionHandler(BaseExecutionHandler):
    def execute(self, df, threshold_correlation):
        col_corr = set()
        corr_matrix = df.corr()
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if (corr_matrix.iloc[i, j] >= threshold_correlation) and (corr_matrix.columns[j] not in col_corr):
                    col_name = corr_matrix.columns[i]
                    col_corr.add(col_name)
                    if col_name in df.columns:
                        del df[col_name]

        return df
