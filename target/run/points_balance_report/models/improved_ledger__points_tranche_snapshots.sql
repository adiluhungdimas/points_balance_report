
      
  
    

  create  table "datamart"."datamart"."improved_ledger__points_tranche_snapshots"
  
  
    as
  
  (
     

SELECT 
  *
FROM
  "datamart"."raw_data"."ledger__points_tranche_snapshots_tenant_a"UNION ALL

 

SELECT 
  *
FROM
  "datamart"."raw_data"."ledger__points_tranche_snapshots_tenant_b"UNION ALL

 

SELECT 
  *
FROM
  "datamart"."raw_data"."ledger__points_tranche_snapshots_tenant_c"
  );
  
  