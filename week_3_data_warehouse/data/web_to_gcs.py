import io
import os 
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "gcs-bq-nytaxi")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    blob.upload_from_filename(local_file)


def change_format(df: pd.DataFrame) -> pd.DataFrame:
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    # change columns format for month 5 and 6
    df['PUlocationID'] = pd.to_numeric(df['PUlocationID'], downcast="float")
    df['DOlocationID'] = pd.to_numeric(df['DOlocationID'], downcast="float")

    return df


def web_to_gcs(year, service):
    for i in range(0, 12):

        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        # fhv/fhv_tripdata_2019-01.csv.gz
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # concat url
        url = f"{init_url}/{service}/{file_name}"
        print(url)
        
        # download it using requests via a pandas df
        df = pd.read_csv(url)
        df.to_csv(file_name)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name)

        # change format
        df_formated = change_format(df)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df_formated.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'fhv')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
