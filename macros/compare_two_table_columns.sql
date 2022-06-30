
{% macro compare_column_from_two_tables(table1, table2, primary_key, column_to_compare) %}
{% set select1 %}
  select * from {{ table1 }}
{% endset %}

{% set select2 %}
  select * from {{ table2 }}
{% endset %}
{{
audit_helper.compare_column_values(
a_query=select1,
b_query=select2,
primary_key=primary_key,
column_to_compare=column_to_compare
)
}}
{% endmacro %}
