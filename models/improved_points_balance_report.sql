{{ 
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'concat(tenant_date, customer_id)',
    full_refresh = false
  )
}}

SELECT 
  {{ var('tenant_date') }} AS tenant_date,
  customer.external_id AS customer_id,
  COALESCE(SUM(balance.points_balance), 0) AS available_points_balance
FROM
  {{ ref('improved_crm__customers') }} AS customer
LEFT JOIN
  {{ ref('improved_ledger__points_tranche_snapshots') }} AS balance
ON
  customer.id = balance.user_id
  AND balance.scd_valid_from at time zone 'aedt' <= {{ var('tenant_date') }}
  AND (balance.scd_valid_to at time zone 'aedt' >= {{ var('tenant_date') }} OR balance.scd_valid_to IS NULL)
  AND balance.expired_on > {{ var('tenant_date') }}
GROUP BY
  1,2