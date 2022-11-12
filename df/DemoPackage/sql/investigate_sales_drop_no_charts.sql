

WITH Sales_Agg AS (
    SELECT
        SUM(m_amount) AS sum_sales_amount,
        dim_region,
        monthname(to_date(ds)) as month
    FROM DEMO_DB.dbt_test.raw_fct_sales
    WHERE dim_product IN ({product}) AND dim_region IN ({region})
    GROUP BY dim_region, month
),
Agg_Customer_Support_Case AS (
    SELECT
        COUNT(1) AS count_case,
        dim_region,
        monthname(to_date(ds)) as month
    FROM DEMO_DB.dbt_test.raw_fct_customer_support_cases
    WHERE dim_product IN ({product}) AND dim_region IN ({region})
    GROUP BY dim_region, month
),
Agg_Campaign AS (
    SELECT
        COUNT(1) as count_campaign,
        dim_region,
        monthname(to_date(ds)) as month
    FROM DEMO_DB.dbt_test.raw_fct_campaign
    WHERE dim_product IN ({product}) AND dim_region IN ({region})
    GROUP BY dim_region, month
),
Agg_Diff_With_Competitor_Price AS (
    SELECT
        SUM(m_price - m_competing_price) as diff_price,
        dim_region,
        monthname(to_date(ds)) as month
    FROM DEMO_DB.dbt_test.raw_fct_competing_product
    WHERE dim_product IN ({product}) AND dim_region IN ({region})
    GROUP BY dim_region, month
)
SELECT
    sum_sales_amount,
    count_campaign,
    diff_price,
    count_case,
    A.dim_region,
    A.month
FROM
    Sales_Agg A
LEFT JOIN
    Agg_Customer_Support_Case B
    ON A.dim_region = B.dim_region AND A.month = B.month
LEFT JOIN
    Agg_Campaign C
    ON A.dim_region = C.dim_region AND A.month = C.month
LEFT JOIN
    Agg_Diff_With_Competitor_Price D
    ON A.dim_region = D.dim_region AND A.month = D.month


