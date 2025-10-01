import pandas as pd
import os

def combine_common_columns_with_age(directory, delimiter="\t"):
    """
    Combines .txt and .tsv files in the current directory using only common columns,
    and standardizes the age column (e.g., 'AGE2', 'AGE3') to 'AGE'.

    Parameters:
    - directory
    - delimiter (str): Delimiter used in the text/tsv files.

    Returns:
    - pd.DataFrame: Combined DataFrame with standardized age column and common columns.
    """
    dfs = []
    common_cols = None
    age_column_candidates = ['AGE2', 'AGE3']

    for file in os.listdir(directory):
        if file.endswith(".txt") or file.endswith(".tsv"):
            file_path = os.path.join(directory, file)
            try:
                df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
                df["source_file"] = file  # optional: track origin

                # Rename age column if it exists
                for age_col in age_column_candidates:
                    if age_col in df.columns:
                        df = df.rename(columns={age_col: "AGE"})
                        break  # Stop once one match is found

                # Update common columns (excluding age — handled separately)
                cols = set(df.columns) - {"source_file"}  # don't require this to be common
                if common_cols is None:
                    common_cols = cols
                else:
                    common_cols &= cols

                dfs.append(df)
            except Exception as e:
                print(f"Failed to read {file}: {e}")

    if not dfs or not common_cols:
        print("No compatible files found.")
        return pd.DataFrame()

    # Ensure AGE and source_file are retained if present
    final_cols = list(common_cols | {"AGE", "source_file"})

    final_dfs = [df[[col for col in final_cols if col in df.columns]] for df in dfs]
    
    return pd.concat(final_dfs, ignore_index=True)


def filter_age(df):
    # Mask for files with '2022' or '2023' in source_file
    recent_mask = df["source_file"].str.contains("2021|2022|2023")
    
    # Subset 1: source_file contains 2022 or 2023 AND age between 4 and 7
    subset_recent = df[recent_mask & df["AGE"].between(4, 7)]
    
    # Subset 2: all other files AND age between 7 and 13
    subset_older = df[~recent_mask & df["AGE"].between(7, 13)]
    
    # Combine the two subsets
    filtered_age = pd.concat([subset_recent, subset_older], ignore_index=True)
    return filtered_age


def filter_cols(df):
    filtered_columns = [col for col in df.columns if col.startswith(('ALC', 'ALD', 'UAD', 'AGE', 'FILEDATE', 'IRSEX', 'source_file', 'AL30'))]
    
    # Create a new DataFrame with just those columns
    df_filtered = df[filtered_columns]
    df_filtered["year"] = df_filtered["source_file"].str.extract(r"(20\d{2})")
    df_filtered = df_filtered[(df_filtered["ALCEVER"] == 1) | (df_filtered["ALCEVER"] == 2)]
    return df_filtered


def preprocess_data():
    df = combine_common_columns_with_age("../data/raw", delimiter="\t")
    df = filter_age(df)
    df = filter_cols(df)
    return df