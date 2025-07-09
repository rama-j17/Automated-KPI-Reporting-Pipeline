-- Raw external tables
CREATE EXTERNAL TABLE transactions_raw (
  txn_id        STRING,
  customer_id   STRING,
  txn_ts        TIMESTAMP,
  merchant_cat  STRING,
  amount_usd    DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/warehouse/transactions_raw';

CREATE EXTERNAL TABLE customers_raw (
  customer_id  STRING,
  gender       STRING,
  age          INT,
  region       STRING,
  industry     STRING,
  join_dt      DATE,
  attrition_flag STRING   -- “Existing Customer” / “Attrited Customer”
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/warehouse/customers_raw';

CREATE EXTERNAL TABLE campaigns_raw (
  campaign_id  STRING,
  start_dt     DATE,
  end_dt       DATE,
  channel      STRING,
  treatment    STRING,
  budget_usd   DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/warehouse/campaigns_raw';

-- Cleaned fact/dim (managed) tables
CREATE TABLE transactions_fact   LIKE transactions_raw
  PARTITIONED BY (txn_ym STRING);
CREATE TABLE customer_dim        LIKE customers_raw;
CREATE TABLE campaign_fact       LIKE campaigns_raw;

-- KPI aggregate (target for BI)
CREATE TABLE kpi_agg_monthly (
  month_ym          STRING,
  segment           STRING,
  treatment         STRING,
  spend_usd         DOUBLE,
  activated_accts   BIGINT,
  total_accts       BIGINT,
  activation_rate   DOUBLE,
  churned_accts     BIGINT,
  churn_rate        DOUBLE,
  roi_pct           DOUBLE
)
PARTITIONED BY (month_ym);
