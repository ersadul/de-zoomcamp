from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = GcpCredentials(
    service_account_info={
  "type": "********",
  "auth_uri": "********",
  "client_id": "********",
  "token_uri": "********",
  "project_id": "********",
  "private_key": "********",
  "client_email": "********",
  "private_key_id": "********",
  "client_x509_cert_url": "********",
  "auth_provider_x509_cert_url": "********"
    }  # enter your credentials info or use the file method.
)
credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket="dte-de-375705-prefect",  # insert your  GCS bucket name
)

bucket_block.save("gcs-zoomcamp", overwrite=True)