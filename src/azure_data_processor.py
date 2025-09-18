import pandas as pd
import json
import hashlib
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

class AzureDataProcessor:
    """Download raw data from Azure, sanitise it, and store locally"""
    
    def __init__(self):
        # Get connection string from environment variable
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = 'raw-inspection-data'
        self.client_mapping = {}
        self.reverse_mapping = {}
    
    def download_from_azure(self, blob_name='raw_inspection_data.csv'):
        """Download raw data from Azure Blob Storage"""
        print(f"Connecting to Azure Blob Storage...")
        
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=self.container_name, 
            blob=blob_name
        )
        
        print(f"Downloading {blob_name} from Azure...")
        download_stream = blob_client.download_blob()
        
        # Save to temporary file
        temp_path = 'data/temp_raw_data.csv'
        with open(temp_path, 'wb') as f:
            f.write(download_stream.readall())
        
        print(f"✓ Downloaded to {temp_path}")
        return temp_path
    
    def _generate_anonymous_id(self, original_name, prefix="Company"):
        """Generate consistent anonymous ID"""
        if original_name not in self.client_mapping:
            hash_val = int(hashlib.md5(original_name.encode()).hexdigest()[:8], 16)
            anonymous_id = f"{prefix}_{hash_val % 1000:03d}"
            self.client_mapping[original_name] = anonymous_id
            self.reverse_mapping[anonymous_id] = original_name
        
        return self.client_mapping[original_name]
    
    def sanitise_data(self, df):
        """Sanitise sensitive client information"""
        df_clean = df.copy()
        
        # Anonymise client and pipeline names
        df_clean['client_name'] = df_clean['client_name'].apply(
            lambda x: self._generate_anonymous_id(x, "Company")
        )
        df_clean['pipeline_name'] = df_clean['pipeline_name'].apply(
            lambda x: self._generate_anonymous_id(x, "Pipeline")
        )
        
        return df_clean
    
    def save_to_sqlite(self, df, db_path='data/pipeline_data.db'):
        """Save sanitised data to SQLite database"""
        import sqlite3
        
        conn = sqlite3.connect(db_path)
        df.to_sql('inspection_data', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✓ Saved {len(df)} records to SQLite database: {db_path}")
    
    def process_pipeline(self):
        """Complete pipeline: Download → Sanitise → Store locally"""
        
        # Step 1: Download from Azure
        temp_file = self.download_from_azure()
        
        # Step 2: Load data
        print("\nLoading data...")
        df = pd.read_csv(temp_file)
        print(f"✓ Loaded {len(df)} records")
        
        # Step 3: Sanitise
        print("\nSanitising data...")
        df_clean = self.sanitise_data(df)
        
        # Step 4: Save to SQLite
        print("\nSaving to local database...")
        self.save_to_sqlite(df_clean)
        
        # Step 5: Save mapping
        print("\nSaving anonymisation mapping...")
        mapping_data = {
            'created_at': datetime.now().isoformat(),
            'client_mapping': self.client_mapping,
            'reverse_mapping': self.reverse_mapping
        }
        
        with open('data/client_mapping.json', 'w') as f:
            json.dump(mapping_data, f, indent=2)
        
        print("✓ Saved mapping to data/client_mapping.json")
        
        # Show results
        print("\n=== BEFORE (Raw from Azure) ===")
        print(df[['client_name', 'pipeline_name', 'risk_level']].head(3))
        
        print("\n=== AFTER (Sanitised Local) ===")
        print(df_clean[['client_name', 'pipeline_name', 'risk_level']].head(3))
        
        # Clean up temp file
        os.remove(temp_file)
        print("\n✓ Data processing complete!")

if __name__ == "__main__":
    processor = AzureDataProcessor()
    processor.process_pipeline()