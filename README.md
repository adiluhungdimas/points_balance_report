# Points Balance Report

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
