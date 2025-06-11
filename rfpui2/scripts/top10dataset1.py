# Extract top 10 rows from dataset1
import pandas as pd
df = pd.read_csv("/opt/ml/processing/input/dataset1.csv")
df.head(10).to_csv("/opt/ml/processing/output/dataset1_top10.csv", index=False)
