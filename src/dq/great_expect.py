# src/dq/great_expect.py
import great_expectations as ge
import pandas as pd

df = pd.read_csv("data/raw/credit_card_transactions.csv", nrows=100_000)
ge_df = ge.from_pandas(df)

ge_df.expect_column_values_to_not_be_null('txn_id')
ge_df.expect_column_values_to_be_between('amount_usd', 0, 100_000)
ge_df.expect_column_values_to_match_regex('txn_ym', r'^\d{4}-\d{2}$')

result = ge_df.validate()
if not result.success:
    raise ValueError("DQ failed – aborting load")
