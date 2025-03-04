import os
from datetime import datetime
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CloudStorageManager:
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.bucket_name = os.getenv('GCP_BUCKET_NAME')
        self.credentials_path = os.getenv('GCP_CREDENTIALS_PATH')
        
        if not all([self.project_id, self.bucket_name, self.credentials_path]):
            raise ValueError("Missing required Google Cloud Storage configuration in environment variables")
        
        # Set credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
    
    def upload_dataframe(self, df: pd.DataFrame, source: str) -> str:
        """
        Upload a DataFrame to Google Cloud Storage.
        
        Args:
            df (pd.DataFrame): DataFrame to upload
            source (str): Source identifier ('toto' or 'unibet')
            
        Returns:
            str: Cloud storage path of the uploaded file
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{source}_data_{timestamp}.csv"
        blob_path = f"odds_data/{source}/{filename}"
        
        # Create a blob
        blob = self.bucket.blob(blob_path)
        
        # Convert DataFrame to CSV and upload
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')
        
        return blob_path
    
    def get_latest_file(self, source: str) -> pd.DataFrame:
        """
        Get the latest file from Google Cloud Storage for a specific source.
        
        Args:
            source (str): Source identifier ('toto' or 'unibet')
            
        Returns:
            pd.DataFrame: DataFrame containing the latest data
        """
        # List all blobs in the source directory
        blobs = list(self.bucket.list_blobs(prefix=f"odds_data/{source}/"))
        
        if not blobs:
            raise FileNotFoundError(f"No files found for source: {source}")
        
        # Get the latest blob based on creation time
        latest_blob = max(blobs, key=lambda x: x.time_created)
        
        # Download and read the CSV
        content = latest_blob.download_as_string()
        return pd.read_csv(pd.io.common.BytesIO(content))

# Initialize the storage manager
storage_manager = None

def get_storage_manager():
    global storage_manager
    if storage_manager is None:
        storage_manager = CloudStorageManager()
    return storage_manager 