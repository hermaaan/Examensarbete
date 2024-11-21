{% macro generate_dataset_name(custom_dataset_name, node) -%}

    {%- set default_dataset = target.dataset -%}
    {%- if custom_dataset_name is none -%}

        {{ default_dataset }}

    {%- else -%}

        {{ custom_dataset_name | trim }}

    {%- endif -%}

{%- endmacro %}
