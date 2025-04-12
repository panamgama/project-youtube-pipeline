import pandas as pd

def format_channel_data_dict(dict):

    numeric_cols = ['subscriberCount', 'videoCount', 'viewCount']

    df = pd.DataFrame(dict)

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')   # Converting to numeric format
    
    return df