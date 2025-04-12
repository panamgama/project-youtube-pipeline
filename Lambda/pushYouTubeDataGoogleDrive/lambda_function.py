import json
from google.oauth2.service_account import Credentials
from read_from_athena import read_from_athena
from read_s3_write_google_sheet import read_s3_write_google_sheet
import gspread
import traceback

def lambda_handler(event, context):

    try:
        ## Reading from Athena - stroing in S3   
        db = 'glue_metadata_db_analysis_files'
        all_data_table = 'all_data_all_dates'
        data_output= 's3://youtube-channel-data-v1-02032025/lambda_query_results'
        bucket_name = 'youtube-channel-data-v1-02032025'

        data_file_name = read_from_athena(db, all_data_table, data_output)
        data_file_key = f'lambda_query_results/{data_file_name}'
        print(data_file_key)
    
    except Exception as e:
        print(f"Error querying Athena: {e}\n{traceback.format_exc()}")
        return {
            "status": "ERROR",
            "error": str(type(e).__name__),
            "cause": str(e)
        }
    try:
        # Initialize Google client
        creds = Credentials.from_service_account_file('credentials_v1.json', scopes=[
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ])

        gc = gspread.authorize(creds)

        all_data_rows = read_s3_write_google_sheet(bucket_name, data_file_key, 'Athena Data Export', gc)


    except Exception as e:
        print(f"Error writing to Google Sheets: {e}\n{traceback.format_exc()}")
        return {
            "status": "ERROR",
            "error": str(type(e).__name__),
            "cause": str(e)
        }

    return {
        'all_data_rows': all_data_rows,
        'status': 'SUCCESS'
    }
