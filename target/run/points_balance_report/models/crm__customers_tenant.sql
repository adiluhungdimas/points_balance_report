
      
  
    

  create  table "datamart"."datamart"."crm__customers_tenant"
  
  
    as
  
  (
     

SELECT 
  *
FROM
  "datamart"."raw_data"."crm__customers_tenant_a"UNION ALL

 

SELECT 
  *
FROM
  "datamart"."raw_data"."crm__customers_tenant_b"UNION ALL

 

SELECT 
  *
FROM
  "datamart"."raw_data"."crm__customers_tenant_c"
  );
  
  