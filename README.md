# Points Balance Report (MVP tenant A) 

- create virtual environment
    ```
    python3 -m venv dbt-env 
    source ~/dbt_env/bin/activate
    ```

- install dbt
    ```
    python -m pip install dbt-postgres
    ```

- install postgres using homebrew and configure database
  ```
  brew install postgresql
  psql -U postgres
  create database datamart;
  \c datamart;
  create schema datamart;
  ```

- clone repository and test configuration
  ```
  git clone https://github.com/adiluhungdimas/points_balance_report.git
  dbt debug
  ```  

- load csv to postgres table
  ```
  dbt seed
  ```   

- execute points_balance_report
  ```
  dbt run --model points_balance_report --vars "tenant_date: DATE('2023-12-1')" --profiles-dir ~/points_balance_report/profiles
  ```
  you can change tenant_date for other execution date

- execute query scenario A,B and C
  You can copy query below to your favorite SQL editor or psql and run them
  ```
  points_balance_report_scenario_a.pgsql
  points_balance_report_scenario_b.pgsql
  points_balance_report_scenario_c.pgsql
  ```

# Points Balance Report (Improved Version)
Prerequisite: configure Points Balance Report (MVP tenant A) 
- install and configure airflow
  ```
  pip install "apache-airflow==2.8.2"
  pip install apache-airflow-providers-postgres
  airflow initdb
  airflow create_user -r Admin -u admin -e admin -f admin -l admin -p admin
  ```
  update postgre default connection in airflow UI (database=datamart)

- create schema and table to store data from tenant  
  ```
  create schema raw_data;
  CREATE TABLE raw_data.crm__customers_tenant_a (
    id VARCHAR(255),
    external_id VARCHAR(255),
    tenant_id VARCHAR(255)
  );
  CREATE TABLE raw_data.crm__customers_tenant_b (
    id VARCHAR(255),
    external_id VARCHAR(255),
    tenant_id VARCHAR(255)
  );
  CREATE TABLE raw_data.crm__customers_tenant_c (
    id VARCHAR(255),
    external_id VARCHAR(255),
    tenant_id VARCHAR(255)
  );
     CREATE TABLE raw_data.ledger__points_tranche_snapshots_tenant_a (
    id text,
    tenant_id text,
    user_id text,
    points_balance integer,
    expired_on date,
    scd_id text,
    scd_updated_at timestamp,
    scd_valid_from timestamp,
    scd_valid_to timestamp
  );
   CREATE TABLE raw_data.ledger__points_tranche_snapshots_tenant_b (
    id text,
    tenant_id text,
    user_id text,
    points_balance integer,
    expired_on date,
    scd_id text,
    scd_updated_at timestamp,
    scd_valid_from timestamp,
    scd_valid_to timestamp
  );
  CREATE TABLE raw_data.ledger__points_tranche_snapshots_tenant_c (
    id text,
    tenant_id text,
    user_id text,
    points_balance integer,
    expired_on date,
    scd_id text,
    scd_updated_at timestamp,
    scd_valid_from timestamp,
    scd_valid_to timestamp
  );
  ```
   


