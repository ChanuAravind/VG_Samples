# Extract top 10 rows from dataset2
import pandas as pd
df = pd.read_csv("/opt/ml/processing/input/dataset2.csv")
df.head(10).to_csv("/opt/ml/processing/output/dataset2_top10.csv", index=False) 
