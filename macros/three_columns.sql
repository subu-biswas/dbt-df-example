{% macro test_charts(table1, column_x, column_y, segment_column) %}

select {{ column_x }}, {{ column_y }}, {{ segment_column }} from {{ table1 }}

{% endmacro %}
