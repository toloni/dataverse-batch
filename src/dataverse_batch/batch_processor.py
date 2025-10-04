import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from tqdm import tqdm
import time

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, client, table_name: str, batch_size: int = 100):
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
    
    def _chunk_data(self, data: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Split data into smaller chunks"""
        return [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
    
    def process_sequential(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process data sequentially using $batch API"""
        logger.info(f"Starting sequential processing for {len(data)} records")
        logger.info(f"Using $batch API with batch size: {self.batch_size}")
        
        chunks = self._chunk_data(data)
        all_results = []
        
        with tqdm(total=len(data), desc="Processing batches") as pbar:
            for i, chunk in enumerate(chunks):
                logger.info(f"Processing batch {i+1}/{len(chunks)} with {len(chunk)} records")
                
                chunk_results = self.client.create_records_batch(self.table_name, chunk)
                all_results.extend(chunk_results)
                pbar.update(len(chunk))
                
                # Small pause to avoid overloading the API
                if i < len(chunks) - 1:  # Don't pause after the last batch
                    time.sleep(1)
        
        return self._create_results_dataframe(all_results, data)
    
    def process_parallel(self, data: List[Dict[str, Any]], workers: int = 10) -> pd.DataFrame:
        """Process data in parallel using $batch API"""
        logger.info(f"Starting parallel processing with {workers} workers for {len(data)} records")
        logger.info(f"Using $batch API with batch size: {self.batch_size}")
        
        chunks = self._chunk_data(data)
        all_results = []
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_chunk = {
                executor.submit(self.client.create_records_batch, self.table_name, chunk): (i, chunk)
                for i, chunk in enumerate(chunks)
            }
            
            # Collect results as they complete
            with tqdm(total=len(data), desc="Processing in parallel") as pbar:
                for future in as_completed(future_to_chunk):
                    chunk_index, chunk = future_to_chunk[future]
                    try:
                        chunk_results = future.result()
                        all_results.extend(chunk_results)
                        logger.info(f"Batch {chunk_index + 1} completed: {len(chunk)} records")
                        pbar.update(len(chunk))
                    except Exception as exc:
                        logger.error(f"Error processing batch {chunk_index + 1}: {exc}")
                        # Add error results for the entire chunk
                        error_results = [{
                            'error': str(exc), 
                            'status': 'error',
                            'record_index': chunk_index * self.batch_size + i
                        } for i in range(len(chunk))]
                        all_results.extend(error_results)
                        pbar.update(len(chunk))
        
        return self._create_results_dataframe(all_results, data)
    
    def _create_results_dataframe(self, results: List[Dict[str, Any]], original_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create DataFrame with consolidated results"""
        df_results = pd.DataFrame(results)
        
        # Add statistics
        success_count = len(df_results[df_results['status'] == 'success'])
        error_count = len(df_results[df_results['status'] == 'error'])
        unknown_count = len(df_results[df_results['status'] == 'unknown'])
        
        logger.info("=" * 50)
        logger.info("FINAL PROCESSING REPORT")
        logger.info("=" * 50)
        logger.info(f"Total records processed: {len(results)}")
        logger.info(f"✅ Successes: {success_count}")
        logger.info(f"❌ Errors: {error_count}")
        logger.info(f"❓ Unknown status: {unknown_count}")
        
        if len(results) > 0:
            success_rate = (success_count / len(results)) * 100
            logger.info(f"📊 Success rate: {success_rate:.2f}%")
        
        if error_count > 0:
            errors = df_results[df_results['status'] == 'error']
            common_errors = errors['error'].value_counts().head(5)
            logger.info("🔍 Top errors:")
            for error, count in common_errors.items():
                logger.info(f"   - {error}: {count} occurrences")
        
        logger.info("=" * 50)
        
        return df_results