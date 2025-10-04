import logging
import pandas as pd
from typing import List, Dict, Any
from .client import DataverseClient
from .batch_processor import BatchProcessor
from .utils import setup_logging, validate_data

class DataverseBatch:
    def __init__(self, dynamics_url: str, client_id: str, client_secret: str, tenant_id: str, 
                 log_level: str = "INFO", log_file: str = None):
        """
        Initialize Dataverse Batch client
        
        Args:
            dynamics_url: Dynamics/Dataverse environment URL
            client_id: Application Client ID
            client_secret: Application Client Secret
            tenant_id: Azure AD Tenant ID
            log_level: Log level (DEBUG, INFO, WARNING, ERROR)
            log_file: Path to log file (optional)
        """
        # Configure logging
        self.logger = setup_logging(log_level, log_file)
        
        self.logger.info("🚀 Initializing DataverseBatch...")
        self.logger.info(f"📝 URL: {dynamics_url}")
        self.logger.info(f"🔑 Client ID: {client_id[:8]}...")
        self.logger.info(f"🏢 Tenant ID: {tenant_id}")
        self.logger.info(f"🔒 SSL Verify: False (using httpx with verify=False)")
        
        # Initialize client
        self.client = DataverseClient(dynamics_url, client_id, client_secret, tenant_id)
        
        # Test connection
        if self.client.test_connection():
            self.logger.info("✅ DataverseBatch initialized and connected successfully!")
        else:
            self.logger.warning("⚠️ DataverseBatch initialized, but connection test failed")
    
    def create_multiple(self, data: List[Dict[str, Any]], table: str, batch_size: int = 100, 
                       parallel: bool = False, workers: int = 10) -> pd.DataFrame:
        """
        Create multiple records in Dataverse using $batch API
        
        Args:
            data: List of dictionaries with record data
            table: Dataverse table/entity name
            batch_size: Number of records per batch (recommended: 100-1000)
            parallel: Whether to execute in parallel
            workers: Number of workers for parallel execution
            
        Returns:
            DataFrame with processing results
        """
        self.logger.info("🎯 Starting batch record creation...")
        self.logger.info(f"📊 Total records: {len(data)}")
        self.logger.info(f"🗂️ Table: {table}")
        self.logger.info(f"📦 Batch size: {batch_size}")
        self.logger.info(f"⚡ Parallel mode: {parallel}")
        if parallel:
            self.logger.info(f"👥 Workers: {workers}")
        self.logger.info(f"🔗 Using API: $batch")
        
        start_time = pd.Timestamp.now()
        
        # Validate data
        validate_data(data, table)
        
        # Initialize processor
        processor = BatchProcessor(self.client, table, batch_size)
        
        # Process data
        if parallel:
            result_df = processor.process_parallel(data, workers)
        else:
            result_df = processor.process_sequential(data)
        
        # Final statistics
        end_time = pd.Timestamp.now()
        duration = (end_time - start_time).total_seconds()
        
        success_count = len(result_df[result_df['status'] == 'success'])
        records_per_second = len(data) / duration if duration > 0 else 0
        
        self.logger.info("🎊 PROCESSING COMPLETED!")
        self.logger.info(f"⏱️ Total time: {duration:.2f} seconds")
        self.logger.info(f"📈 Speed: {records_per_second:.2f} records/second")
        self.logger.info(f"✅ Successes: {success_count}/{len(data)}")
        
        return result_df
    
    def get_batch_recommendations(self, total_records: int) -> Dict[str, Any]:
        """
        Return batch configuration recommendations
        """
        recommendations = {
            "batch_size": min(1000, max(100, total_records // 10)),
            "parallel": total_records > 1000,
            "workers": min(10, max(2, total_records // 500))
        }
        
        self.logger.info("💡 Processing recommendations:")
        self.logger.info(f"   Batch size: {recommendations['batch_size']}")
        self.logger.info(f"   Parallel processing: {recommendations['parallel']}")
        self.logger.info(f"   Workers: {recommendations['workers']}")
        
        return recommendations