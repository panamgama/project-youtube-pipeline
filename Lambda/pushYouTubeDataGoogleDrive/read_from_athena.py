import boto3
import time

def read_from_athena(source_db, source_table, output_s3_path):

    query = f'SELECT * FROM "{source_db}"."{source_table}"'
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': source_db
        },
        ResultConfiguration={
            'OutputLocation': output_s3_path,
        }
    )
    
    execution_id = response['QueryExecutionId']

    # Wait for the query to complete
    state = 'RUNNING'
    while state in ['RUNNING', 'QUEUED']:
        response = client.get_query_execution(QueryExecutionId=execution_id)
        state = response['QueryExecution']['Status']['State']
        time.sleep(2)
        print(state)

    

    return f'{execution_id}.csv'
