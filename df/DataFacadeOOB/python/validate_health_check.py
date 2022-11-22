import json


from dft.base_execution_handler import BaseExecutionHandler


class ExecutionHandler(BaseExecutionHandler):
    def execute(self, health_stats_string, threshold_health_percentage):
        try:
            health_stats = json.loads(health_stats_string)

            assert health_stats["TableStat"]["Health"] >= threshold_health_percentage/100.0
        except Exception as e:
            print("Health Check not satisfied. Failing action")
            raise

        return "Check Passed"
