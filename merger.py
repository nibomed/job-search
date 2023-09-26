import sys
import pandas as pd

def merge_csv(files):
    df_list = []
    for file in files:
        df = pd.read_csv(file)
        df_list.append(df)
    result = pd.concat(df_list)
    result.to_csv('result.csv', index=False)

if __name__ == '__main__':
    files = sys.argv[1:]
    merge_csv(files)
