SELECT 
  tenant_date,
  customer_id,
  available_points_balance
FROM
  datamart.points_balance_report
WHERE
  tenant_date BETWEEN '2023-11-30' AND '2023-12-01'
ORDER BY
  tenant_date, customer_id ASC    