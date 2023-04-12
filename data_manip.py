import pandas as pd


sample_file = pd.read_csv("samples_extremely_large.tsv", sep="\s+", dtype=str).set_index("sample", drop=False)
sample_list = sample_file["sample"].values.tolist()

s = set()
for i in sample_list:
    if i not in s:
        s.add(i)
    else:
        print(i)