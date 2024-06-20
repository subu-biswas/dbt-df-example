SELECT "Store_type",
sum(ABS("Qty")) as total_sales,
sum(ABS(total_amt)) as total_revenue,
sum(ABS(total_amt)) - sum(ABS(Tax)) as profit
FROM {{table_name}}
WHERE TO_TIMESTAMP(tran_date, 'YYYY-MM-DD') BETWEEN (CURRENT_DATE - INTERVAL '4 weeks') AND CURRENT_DATE
GROUP BY "Store_type";