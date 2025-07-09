# scripts/generate_campaign_costs.py
from faker import Faker; import pandas as pd, random, uuid
fake = Faker()
records = []
for _ in range(200):
    cid = str(uuid.uuid4())[:8]
    start = fake.date_between('-365d', '-30d')
    end   = fake.date_between(start, '+60d')
    budget = round(random.uniform(5_000, 200_000), 2)
    records.append([cid, start, end,
                    random.choice(['Email','SMS','Paid Social','Events']),
                    random.choice(['Offer','Control']),
                    budget])
df = pd.DataFrame(records,
      columns=['campaign_id','start_dt','end_dt','channel',
               'treatment','budget_usd'])
df.to_csv('data/raw/campaign_costs.csv', index=False)
