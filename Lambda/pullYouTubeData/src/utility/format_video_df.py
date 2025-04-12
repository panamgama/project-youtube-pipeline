import pandas as pd
from dateutil import parser
import isodate

def format_video_df (base_df):
    """
    Format table containing details of YT videos

    Params:
        base_df : Dataframe containing raw details of video statistics from YT API
            'channelTitle', 'title', 'description', 'tags', 'publishedAt',
            'viewCount', 'likeCount', 'favoriteCount', 'commentCount',
            'duration', 'definition', 'caption'.

    Returns:
        DataFrame containing additional columns specified below:
            Numeric cols: all columns with a count value set to numeric type
            Date cols: all date columns converted to date type, timezone extracted and time converted to UTC
            Time cols: duration columns converted to seconds
            Like ratio: Number of likes to a 100 videos
            Comment ratio: Number of comments to a 100 videos

    """
    numeric_cols = ['viewCount', 'likeCount', 'commentCount']

    time_cols = ['duration']

    date_cols = ['publishedAt']

    
    for col in numeric_cols:
        base_df[col] = base_df[col].apply(pd.to_numeric, errors='coerce')   # Converting to numeric format

    for col in date_cols:
        base_df[col] =  base_df[col].apply(lambda x: parser.parse(x))   # Converting to datetime format
        base_df[f'{col}_dayName'] = base_df[col].apply(lambda x: x.strftime("%A"))  # Extracting day from original date
        base_df[f'{col}_in_utc'] = base_df[col].dt.tz_convert('UTC').dt.tz_localize(None)   # Converting time to UTC and omitting timezone
        base_df[f'{col}_timezone'] = base_df[col].dt.tz     # Extracting timezone of the original datetime

    for col in time_cols:
        base_df[f'{col}_parsed'] = base_df[col].apply(lambda x: isodate.parse_duration(x))  # Converting to time format
        base_df[f'{col}_seconds'] = base_df[f'{col}_parsed'].dt.total_seconds()     # Calculating duration in seconds
    
    # Counting tags of a video
    base_df['tagsCount'] = base_df['tags'].apply(lambda x: 0 if x is None else len(x))  # If no tags -> value: 0

    # Calculating comments and likes per 1000 view ratio
    base_df['likeRatio'] = base_df['likeCount']/ base_df['viewCount'] * 1000
    base_df['commentRatio'] = base_df['commentCount']/ base_df['viewCount'] * 1000

    return base_df.drop(columns = date_cols) # Omitting origiinal date columns (they contain timezones)