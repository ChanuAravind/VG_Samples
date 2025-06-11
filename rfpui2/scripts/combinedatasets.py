# Combine the two datasets
import pandas as pd

df1 = pd.read_csv("/opt/ml/processing/input1/dataset1_top10.csv")
df2 = pd.read_csv("/opt/ml/processing/input2/dataset2_top10.csv")

df_combined = pd.concat([df1, df2], axis=1)
df_combined.to_csv("/opt/ml/processing/output/combined.csv", index=False)
