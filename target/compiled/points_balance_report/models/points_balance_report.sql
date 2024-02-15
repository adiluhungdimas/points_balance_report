

SELECT 
  DATE('2023-12-1') AS tenant_date,
  customer.external_id AS customer_id,
  COALESCE(SUM(balance.points_balance), 0) AS available_points_balance
FROM
  "datamart"."datamart"."crm__customers" AS customer
LEFT JOIN
  "datamart"."datamart"."ledger__points_tranche_snapshots" AS balance
ON
  customer.id = balance.user_id
  AND balance.scd_valid_from at time zone 'aedt' <= DATE('2023-12-1')
  AND (balance.scd_valid_to at time zone 'aedt' >= DATE('2023-12-1') OR balance.scd_valid_to IS NULL)
  AND balance.expired_on > DATE('2023-12-1')
GROUP BY
  1,2