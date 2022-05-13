{% test relationships(model, column_name, to, field) %}
    {{ glue__test_relationships(model, column_name, to, field) }}
{% endtest %}

{% test accepted_values(model, column_name, values, quote=True) %}
    {{ glue__test_accepted_values(model, column_name, values, quote) }}
{% endtest %}
