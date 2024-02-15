
      
        
            delete from "datamart"."datamart"."points_balance_report"
            where (
                concat(tenant_date, customer_id)) in (
                select (concat(tenant_date, customer_id))
                from "points_balance_report__dbt_tmp134309479080"
            );

        
    

    insert into "datamart"."datamart"."points_balance_report" ("tenant_date", "customer_id", "available_points_balance")
    (
        select "tenant_date", "customer_id", "available_points_balance"
        from "points_balance_report__dbt_tmp134309479080"
    )
  