{% macro analyze_trends(input_table, dim_column, target_column, agg_function)}

select {{ agg_function }}({{ target_column }}) AS "result", {{ dim_column }} from {{ input_table }} group by {{ dim_column }}

{%endmacro}