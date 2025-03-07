# import boto3
# import pandas as pd
# import io

# def read_parquet_from_s3(bucket_name, key):
#     """
#     Reads a Parquet file from S3 and returns it as a Pandas DataFrame.

#     Args:
#         bucket_name (str): The name of the S3 bucket.
#         key (str): The key of the Parquet file in S3.

#     Returns:
#         pandas.DataFrame: The Parquet file data as a Pandas DataFrame, or None if an error occurs.
#     """
#     s3 = boto3.client('s3')
#     try:
#         response = s3.get_object(Bucket=bucket_name, Key=key)
#         parquet_content = response['Body'].read()
        
#         df = pd.read_parquet(io.BytesIO(parquet_content))
#         return df
#     except Exception as e:
#         print(f"Error reading Parquet file from S3: {e}")
#         return None

import polars as pl
from tests.bus_performance_manager.test_tm_ingestion import check_stop_crossings

if __name__ == '__main__':



    # Read a Parquet file into a DataFrame
    df = pl.read_parquet("/Users/hhuang/Downloads/20240104.parquet")
    # out = check_stop_crossings(stop_crossings_filepath= '/Users/hhuang/Downloads/120250215.parquet')

    # breakpoint()

    # # Display the DataFrame
    # print(df)

# import pandas as pd
# import fastparquet as fp
# import boto3


# def read_s3_file(bucket_name, object_key):
#     """
#     Reads and returns the content of a file from an S3 bucket.

#     Args:
#         bucket_name (str): The name of the S3 bucket.
#         object_key (str): The key (path) of the object within the bucket.

#     Returns:
#         bytes: The content of the file as bytes, or None if an error occurs.
#     """
#     s3_client = boto3.client('s3')
#     try:
#         response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
#         return response['Body'].read()
#     except Exception as e:
#          print(f"Error reading {object_key} from {bucket_name}: {e}")
#          return None

# # Example usage:
# bucket_name = "your-bucket-name"
# object_key = "path/to/your/object.txt"

# file_content = read_s3_file(bucket_name, object_key)

# if file_content:
#     # Process the file content (e.g., decode if it's text)
#     df = pd.read_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/TM/STOP_CROSSING/120240207.parquet', engine='fastparquet')
#     # df.to_csv('out.csv', index=False)
#     print(df.head())


# if __name__ == "__main__":

#     # Replace 'your_file.parquet' with the actual file path
#     df = pd.read_parquet('/Users/hhuang/Downloads/120250204.parquet', engine='fastparquet')
#     # df.to_csv('out.csv', index=False)
#     print(df.head())

    
#     # s3://mbta-ctd-dataplatform-springboard/lamp/TM/STOP_CROSSING/120240207.parquet

#     row_count = len(df)
#     print(row_count) # Output: 3

#     # Get column names as an Index object
#     column_names = df.columns

#     # Convert column names to a list
#     column_names_list = df.columns.tolist()

#     print(column_names)
#     print(column_names_list)


#     # Count occurrences of each value in 'col1'
#     value_counts = df['VEHICLE_ID'].value_counts()

#     # Print the result
#     print(value_counts)

#     print("NAN_COUNTS_____HHHH\n")
#     nan_counts = df.isna().sum()
#     print(nan_counts)

#     # STOP_CROSSING_ID              0
#     # TRIP_GEO_NODE_XREF_ID         0
#     # PATTERN_GEO_NODE_SEQ          0
#     # CALENDAR_ID                   0
#     # ROUTE_DIRECTION_ID            2
#     # PATTERN_ID                    2
#     # GEO_NODE_ID                   0
#     # BLOCK_STOP_ORDER              0
#     # SCHEDULED_TIME                0
#     # ACT_ARRIVAL_TIME         291012
#     # ACT_DEPARTURE_TIME       291012
#     # ODOMETER                 291012
#     # WAIVER_ID                367291
#     # DAILY_WORK_PIECE_ID           0
#     # TIME_POINT_ID            282135
#     # SERVICE_TYPE_ID               0
#     # VEHICLE_ID               291012
#     # TRIP_ID                    3307
#     # PULLOUT_ID                    0
#     # IsRevenue                  1652
#     # SCHEDULE_TIME_OFFSET     373682
#     # CROSSING_TYPE_ID              0
#     # OPERATOR_ID              291012
#     # CANCELLED_FLAG                0
#     # IS_LAYOVER                    0
#     # ROUTE_ID                      2