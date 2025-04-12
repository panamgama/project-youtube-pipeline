from io import BytesIO
import boto3

# Initialize S3 client
s3_client = boto3.client("s3")

def upload_df_to_s3(df, bucket_name, file_key):
    """Uploads a Pandas DataFrame to S3 as a Parquet file."""
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=parquet_buffer.getvalue())

# Uses Parquet format → Faster queries, better compression.
# Uses pyarrow engine → Recommended for compatibility with AWS Athena.
# Applies snappy compression → Efficient storage without heavy processing overhead.