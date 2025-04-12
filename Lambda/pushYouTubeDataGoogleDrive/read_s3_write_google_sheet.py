import boto3
import pandas as pd
import time

def read_s3_write_google_sheet(bucket_name, file_key, sheet_name, client_object, read_chunksize=10000, write_chunk_size=2500):
    s3 = boto3.client('s3')

    # Read the object from S3 into memory
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = response['Body']  # This is a file-like object

    # Open Google Sheet
    sheet = client_object.open(sheet_name)
    worksheet = sheet.sheet1

    # Clear previous content in the Google Sheet
    worksheet.clear()

    # Iterate over chunks of the CSV file
    header_written = False  # Flag to check if header is written to Google Sheets
    current_rows = 0

    for chunk in pd.read_csv(body, chunksize=read_chunksize):
        # Replace inf values and NaNs in the chunk
        chunk.replace([float('inf'), float('-inf')], 0, inplace=True)
        chunk = chunk.where(pd.notnull(chunk), 0)

        # Write header (only once, before the first chunk)
        if not header_written:
            print("Writing header...")
            worksheet.resize(cols=len(chunk.columns)) # Set required columns only
            worksheet.update(range_name='A1', values=[chunk.columns.tolist()])
            header_written = True
            current_rows += 1

        # Write data chunk to the Google Sheet
        for i in range(0, len(chunk), write_chunk_size):
            data_chunk = chunk.iloc[i:i + write_chunk_size]
            required_rows = current_rows + len(data_chunk)
            
            # Resize sheet if needed
            if required_rows > worksheet.row_count:
                worksheet.resize(rows=required_rows)

            cell_range = f'A{current_rows + 1}'  # Offset by 1
            worksheet.update(range_name=cell_range, values=data_chunk.values.tolist())
            current_rows += len(data_chunk)
            
            print(f"{len(data_chunk)} rows written - from {current_rows +1 - len(data_chunk)} to {current_rows}")
            print('Sleeping for 5 seconds...')
            time.sleep(5)
        

    return current_rows
