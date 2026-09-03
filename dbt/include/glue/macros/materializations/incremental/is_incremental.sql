{% macro is_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {#-- pass file_format so relations like s3tables resolve correctly (GitHub #620) #}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table, config.get('file_format')) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'incremental'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}
