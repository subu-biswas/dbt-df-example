SELECT {column_name}, Count({column_name}) FROM {table_name} GROUP BY {column_name} ORDER BY Count({column_name}) LIMIT 10