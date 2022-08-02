{% macro investigate_sales_drop(region, product) %}

WITH Sales_Agg AS (
    SELECT
        SUM(m_amount) AS sum_sales_amount,
        dim_region,
        ds
    FROM {{ ref("raw_fct_sales") }}
    WHERE dim_region = {{ region }} AND dim_product = {{ product }}
    GROUP BY dim_region, ds
),
Agg_Customer_Support_Case AS (
    SELECT
        COUNT(1) AS count_case,
        dim_region,
        ds
    FROM {{ ref("raw_fct_customer_support_cases") }}
    WHERE dim_region = {{ region }} AND dim_product = {{ product }}
    GROUP BY dim_region, ds
),
Agg_Campaign AS (
    SELECT
        COUNT(1) as count_campaign,
        dim_region,
        ds
    FROM {{ ref("raw_fct_campaign") }}
    WHERE dim_region = {{ region }} AND dim_product={{ product }}
    GROUP BY dim_region, ds
),
Agg_Diff_With_Competitor_Price AS (
    SELECT
        SUM(m_competing_price - m_price) as diff_price,
        dim_region,
        ds
    FROM {{ ref("raw_fct_competing_product") }}
    WHERE
        dim_product = {{ product }} AND
        dim_region = {{ region }}
    GROUP BY dim_region, ds
)
SELECT
    sum_sales_amount,
    count_campaign,
    diff_price,
    count_case,
    A.dim_region,
    A.ds
FROM
    Sales_Agg A
LEFT JOIN
    Agg_Customer_Support_Case B
    ON A.dim_region = B.dim_region AND A.ds = B.ds
LEFT JOIN
    Agg_Campaign C
    ON A.dim_region = C.dim_region AND A.ds = C.ds
LEFT JOIN
    Agg_Diff_With_Competitor_Price D
    ON A.dim_region = D.dim_region AND A.ds = D.ds


{% endmacro %}
