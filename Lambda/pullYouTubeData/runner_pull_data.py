import pandas as pd
from pandas import json_normalize  # Updated for compatibility
from datetime import datetime
import requests
import traceback
from googleapiclient.discovery import build

from src.process.get_channel_details import get_channel_details
from src.process.get_video_ids import get_all_video_ids
from src.process.get_video_details import get_video_details
from src.utility.format_channel_data_dict import format_channel_data_dict
from src.utility.upload_df_to_s3 import upload_df_to_s3
from src.data.api_key import api_key
from src.data.channel_id_list import channel_id_list
from src.data.s3_bucket_name import s3_bucket_name

def lambda_handler(event, context):

    print("Function loaded")

    # Initiating
    api_key_from_file = api_key
    channel_ids = channel_id_list
    youtube = build('youtube', 'v3', developerKey=api_key_from_file)
    
    print("Function initialized")

    # Initialize empty DataFrames to prevent reference errors
    video_data_df = pd.DataFrame()
    channel_data_df = pd.DataFrame()

    try:
        # Get channel data for specified channels - returned as a dictionary
        channel_data = get_channel_details(youtube, channel_ids)
        print("Channel data pulled")

        if not channel_data:
            return {
                "status": "ERROR",
                "error": "No channel data found",
                "cause": "No channel data found"
            }
    except Exception as e:
        print(f"Error fetching channel data: {e}\n{traceback.format_exc()}")
        return {
            "status": "ERROR",
            "error": str(type(e).__name__),
            "cause": str(e)
        }

    try:
        video_data = []

        for channel in channel_data:
            if "channelId" not in channel or "uploadPlaylistId" not in channel:
                print(f"Skipping channel {channel} due to missing keys")
                continue

            videoIDList = get_all_video_ids(youtube, channel["uploadPlaylistId"])

            if not videoIDList:
                print(f"No videos found for channel {channel['channelId']}")
                continue

            video_details = {
                "channelId": channel["channelId"],
                "videoDetails": get_video_details(youtube, videoIDList)
            }
            video_data.append(video_details)

        print("Video data pulled")

        if not video_data:
            return {
                "status": "ERROR",
                "error": "No video data found",
                "cause": "No video data found"
            }

        video_data_df = json_normalize(video_data, record_path=["videoDetails"], meta=["channelId"])
        print("Video df created")

        channel_data_df = format_channel_data_dict(channel_data)
        print("Channel df created")

    except Exception as e:
        print(f"Error processing video data: {e}\n{traceback.format_exc()}")
        return {
            "status": "ERROR",
            "error": str(type(e).__name__),
            "cause": str(e)
        }

    try:
        current_date = datetime.now().strftime("%Y/%m/%d")
        bucket_name = s3_bucket_name
        file_key_channel_data = f"raw/{current_date}/channel_data.parquet"
        file_key_video_data = f"raw/{current_date}/video_data.parquet"
        print("Bucket keys initialized")

        if not video_data_df.empty:
            upload_df_to_s3(video_data_df, bucket_name, file_key_video_data)
            print("Video data written to bucket")
        else:
            print("Skipping video data upload: No data available")

        if not channel_data_df.empty:
            upload_df_to_s3(channel_data_df, bucket_name, file_key_channel_data)
            print("Channel data written to bucket")
        else:
            print("Skipping channel data upload: No data available")

    except Exception as e:
        print(f"Error uploading data to S3: {e}\n{traceback.format_exc()}")
        return {
            "status": "ERROR",
            "error": str(type(e).__name__),
            "cause": str(e)
        }

    return {"status": "SUCCESS"}
