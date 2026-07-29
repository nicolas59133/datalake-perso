{#
    Par défaut dbt concatène "<schéma cible>_<custom_schema>". On veut le schéma
    custom tel quel (ex: "gold" plutôt que "silver_gold" pour une future couche),
    override standard documenté par dbt.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
