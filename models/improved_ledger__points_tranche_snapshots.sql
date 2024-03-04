{{ 
  config(
    materialized = 'table',
  )
}}

{%- for tenant in ['tenant_a', 'tenant_b', 'tenant_c'] %}
{%- set table_name = 'ledger__points_tranche_snapshots_' + tenant %} 

SELECT 
  *
FROM
  {{ source('raw_data', table_name) }}
{%- if not loop.last -%}
UNION ALL

{% endif -%}
{% endfor -%}  

