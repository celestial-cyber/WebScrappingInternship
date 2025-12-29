import pandas as pd
import glob
import os

# Current folder (Data)
FOLDER_PATH = "."

excel_files = [
    f for f in glob.glob(os.path.join(FOLDER_PATH, "*.xlsx"))
    if not os.path.basename(f).startswith("~$")
]

print("Excel files found:", excel_files)

all_data = []

for file in excel_files:
    df = pd.read_excel(file)
    df["Source_File"] = os.path.basename(file)
    all_data.append(df)

merged_df = pd.concat(all_data, ignore_index=True)
merged_df.drop_duplicates(inplace=True)

merged_df.to_excel("ALL_COLLEGEDUNIA_DATA.xlsx", index=False)

print(f"✅ Merged {len(excel_files)} Excel files into ALL_COLLEGEDUNIA_DATA.xlsx")
