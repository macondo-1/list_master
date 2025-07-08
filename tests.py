import sqlite3
import os
import pandas as pd
import glob

os.chdir("/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process")
file_extension = '.csv'
all_filenames = [i for i in glob.glob(f"*{file_extension}")]

deduper_list_path = '/Users/albertoruizcajiga/python/email_bison_api/test.csv'

df_dedupe = pd.read_csv(deduper_list_path, low_memory=False)
#print(df_dedupe.email)

for filename in all_filenames:
    #print(filename)
    df = pd.read_csv(filename)
    df = df[~df.email.isin(df_dedupe.email)]

    df.to_csv(filename, index=False)

